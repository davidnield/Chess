"""Hard menu restriction, applied after engine admission and before lookahead/optimization."""

def menu_veto(out, sans, allowed):
    parents=out["node_keys"][out["node_our"]]
    veto={}
    for i,h in enumerate(parents):
        lo,hi=out["c_ptr"][i:i+2]
        blocked=[int(code) for code in out["c_san"][lo:hi]
                 if allowed.get(int(h))!=sans[code].rstrip("+#")]
        if blocked: veto[int(h)]=blocked
    return veto

def merge_veto(first,second):
    result={h:list(codes) for h,codes in first.items()}
    for h,codes in second.items():
        result[h]=sorted(set(result.get(h,()))|set(codes))
    return result
