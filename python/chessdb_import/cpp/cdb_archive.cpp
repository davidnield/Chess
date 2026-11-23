// Lossless logical archive source. inventory DB; chunk DB START_HEX END_HEX|- MAX_ROWS.
// Binary stdout for chunk: CDBARC02 + five length-framed fields per record.
// Independently computes original-byte and decoded-stream SHA256 before transport.
#include <algorithm>
#include <array>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>
// Minimal declarations of OpenSSL 3's public EVP ABI. This host has libcrypto.so.3,
// but not the development headers. No private structs are accessed.
extern "C" {
struct evp_md_ctx_st;struct evp_md_st;struct engine_st;
using EVP_MD_CTX=evp_md_ctx_st;using EVP_MD=evp_md_st;
EVP_MD_CTX *EVP_MD_CTX_new();void EVP_MD_CTX_free(EVP_MD_CTX*);
const EVP_MD *EVP_sha256();
int EVP_DigestInit_ex(EVP_MD_CTX*,const EVP_MD*,engine_st*);
int EVP_DigestUpdate(EVP_MD_CTX*,const void*,size_t);
int EVP_DigestFinal_ex(EVP_MD_CTX*,unsigned char*,unsigned int*);
}
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "cdbdirect.h"
#include "fen2cdb.h"
using namespace TERARKDB_NAMESPACE;
enum class STM { WHITE, BLACK, NONE };
enum class MinPlyType { INIT, SINGLE, DUAL, NONE };
struct CDB { DB *db; MinPlyType min_ply_type; };
int backprop_score(int);
std::vector<std::pair<std::string,int>> value_to_scoredMoves(const std::string&,STM,STM&,MinPlyType);
static std::string hex(const std::string&s) {static const char*d="0123456789abcdef";std::string o(s.size()*2,'0');for(size_t i=0;i<s.size();++i){auto b=(unsigned char)s[i];o[2*i]=d[b>>4];o[2*i+1]=d[b&15];}return o;}
static std::string unhex(const std::string&s) {if(s.size()%2)throw std::runtime_error("odd hex");std::string r;for(size_t i=0;i<s.size();i+=2){auto p=s.substr(i,2);size_t n;int v=std::stoi(p,&n,16);if(n!=2||v<0||v>255)throw std::runtime_error("bad hex");r+=char(v);}return r;}
struct Hash {
 EVP_MD_CTX*c=EVP_MD_CTX_new();Hash(){if(!c||!EVP_DigestInit_ex(c,EVP_sha256(),nullptr))throw std::runtime_error("SHA init");}
 ~Hash(){EVP_MD_CTX_free(c);}void add(const char*p,size_t n){if(!EVP_DigestUpdate(c,p,n))throw std::runtime_error("SHA update");}
 void field(const std::string&s){char b[4];uint32_t n=s.size();for(int i=0;i<4;++i)b[i]=char(n>>(8*i));add(b,4);add(s.data(),s.size());}
 std::string finish(){unsigned char b[32];unsigned n;if(!EVP_DigestFinal_ex(c,b,&n)||n!=32)throw std::runtime_error("SHA final");return hex(std::string((char*)b,32));}
};
static void put(std::ostream&o,const std::string&s){if(s.size()>16*1024*1024)throw std::runtime_error("oversize blob");uint32_t n=s.size();for(int i=0;i<4;++i)o.put(char(n>>(8*i)));o.write(s.data(),s.size());if(!o)throw std::runtime_error("stream write");}
static std::array<std::string,3> decode(CDB*c,const std::string&key,const std::string&value) {
 if(key.size()<2||key[0]!='h')return {"","",""};
 std::string fen=cbhexfen2fen(hex(key.substr(1))),moves;bool white=fen.find(" w ")!=std::string::npos;
 // Match cdbdirect's canonical-key orientation, preserving the opaque source bytes separately.
 std::vector<StrPair> pairs;if(!value.empty())get_hash_values(value,pairs);std::vector<std::pair<std::string,int>> scored;
 int wp=-1,bp=-1;
 for(auto&p:pairs){int v=std::stoi(p.second);if(p.first=="a0a0"){
   if(c->min_ply_type==MinPlyType::DUAL){wp=std::max(2*(v>>8)-2,-1);bp=2*(v&255)-1;}
   else if(c->min_ply_type==MinPlyType::SINGLE){v=std::max(v,-1);if(v>=0){wp=v%2?v+1:v;bp=v%2?v:v+1;}}
   else if(c->min_ply_type!=MinPlyType::NONE)throw std::runtime_error("uninitialized ply encoding");
 }else scored.push_back({p.first,backprop_score(v)});}
 std::sort(scored.begin(),scored.end(),[](auto&a,auto&b){return a.second>b.second;});
 for(auto&m:scored)moves+=m.first+":"+std::to_string(m.second)+";";
 // cdbdirect treats an empty value as an absent probe; retain its sentinel exactly in the derived field.
 int canonical=value.empty()?-2:(white?wp:bp),mirror=value.empty()?-2:(white?bp:wp);
 moves+="a0a0:"+std::to_string(canonical)+";";
 return {fen,moves,std::to_string(mirror)};
}
int main(int argc,char**argv){try{
 if(argc<3)throw std::runtime_error("inventory DB | chunk DB START_HEX END_HEX|- MAX_ROWS");
 auto h=cdbdirect_initialize(argv[2]);auto c=reinterpret_cast<CDB*>(h);auto cmp=c->db->GetOptions().comparator;
 if(std::string(cmp->Name())!="leveldb.BytewiseComparator")throw std::runtime_error("unsupported comparator");
 ReadOptions ro;ro.verify_checksums=true;ro.fill_cache=false;std::unique_ptr<Iterator>it(c->db->NewIterator(ro));
 if(std::string(argv[1])=="inventory") {
   it->SeekToFirst();if(!it->Valid())throw std::runtime_error("empty source");std::string first=it->key().ToString();it->SeekToLast();std::string last=it->key().ToString();
   std::vector<LiveFileMetaData>files;c->db->GetLiveFilesMetaData(&files);std::vector<std::string>bounds{first};
   for(auto&f:files){it->Seek(f.smallestkey);if(it->Valid()&&cmp->Compare(it->key(),first)>0)bounds.push_back(it->key().ToString());}
   std::sort(bounds.begin(),bounds.end());bounds.erase(std::unique(bounds.begin(),bounds.end()),bounds.end());
   std::cout<<"{\"schema\":\"chessdb-source-ranges-v1\",\"estimated_records\":"<<cdbdirect_size(h)<<",\"comparator\":\""<<cmp->Name()<<"\",\"min_ply_type\":"<<int(c->min_ply_type)<<",\"first_key\":\""<<hex(first)<<"\",\"last_key\":\""<<hex(last)<<"\",\"starts\":[";
   for(size_t i=0;i<bounds.size();++i){if(i)std::cout<<",";std::cout<<"\""<<hex(bounds[i])<<"\"";}std::cout<<"]}\n";
 }else if(std::string(argv[1])=="chunk"){
   if(argc!=6)throw std::runtime_error("chunk arguments");std::string start=unhex(argv[3]),end;bool bounded=std::string(argv[4])!="-";if(bounded)end=unhex(argv[4]);auto cap=std::stoull(argv[5]);if(!cap)throw std::runtime_error("zero cap");
   if(bounded&&cmp->Compare(start,end)>=0)throw std::runtime_error("invalid range");
   std::vector<char>buffer(8<<20);std::ofstream out;out.rdbuf()->pubsetbuf(buffer.data(),buffer.size());out.open("/dev/stdout",std::ios::binary);out.write("CDBARC02",8);
   Hash raw,stream;uint64_t count=0;std::string first,last;
   for(it->Seek(start);it->Valid()&&(!bounded||cmp->Compare(it->key(),end)<0)&&count<cap;it->Next(),++count){
     auto key=it->key().ToString(),value=it->value().ToString();if(!count)first=key;else if(cmp->Compare(last,key)>=0)throw std::runtime_error("nonmonotone keys");last=key;
     raw.field(key);raw.field(value);auto d=decode(c,key,value);
     if(!d[0].empty() && (count<128 || count%1000000==0)){
       STM stm=d[0].find(" w ")!=std::string::npos?STM::WHITE:STM::BLACK,target=stm;
       auto ref=value_to_scoredMoves(value,stm,target,c->min_ply_type);std::string text;
       for(auto&m:ref)text+=m.first+":"+std::to_string(m.second)+";";
       if(text!=d[1])throw std::runtime_error("canonical decode reference mismatch");
       target=stm==STM::WHITE?STM::BLACK:STM::WHITE;auto other=value_to_scoredMoves(value,stm,target,c->min_ply_type);
       if(std::to_string(other.back().second)!=d[2])throw std::runtime_error("mirror decode reference mismatch");
     }
     for(auto&s:std::array<std::string,5>{key,value,d[0],d[1],d[2]}){stream.field(s);put(out,s);}
   }
   if(!it->status().ok())throw std::runtime_error(it->status().ToString());
   bool done=!it->Valid()||(bounded&&cmp->Compare(it->key(),end)>=0);std::string next=done?"":it->key().ToString();out.close();if(!out)throw std::runtime_error("failed stream flush");
   std::cerr<<"{\"records\":"<<count<<",\"first_key_hex\":\""<<hex(first)<<"\",\"last_key_hex\":\""<<hex(last)<<"\",\"next_key_hex\":"<<(done?"null":"\""+hex(next)+"\"")<<",\"range_done\":"<<(done?"true":"false")<<",\"source_logical_sha256\":\""<<raw.finish()<<"\",\"stream_sha256\":\""<<stream.finish()<<"\"}\n";
 }else throw std::runtime_error("unknown mode");
 if(!it->status().ok())throw std::runtime_error(it->status().ToString());it.reset();cdbdirect_finalize(h);return 0;
}catch(const std::exception&e){std::cerr<<e.what()<<"\n";return 2;}}
