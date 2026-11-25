use book_builder_rs::optimizer::{
    benchmark::{self, Config},
    fixtures,
    snapshot::Snapshot,
    Result,
};
use std::{env, fs, path::Path};
fn execute() -> Result<()> {
    let a: Vec<_> = env::args().collect();
    match a.get(1).map(String::as_str) {
        Some("config") if a.len()==3=>benchmark::write_new(Path::new(&a[2]),&Config::default()),
        Some("plan") if a.len()==3=>benchmark::write_new(Path::new(&a[2]),&benchmark::study_plan()),
        Some("fixtures") if a.len()==3=>{
            let d=Path::new(&a[2]);fs::create_dir_all(d).map_err(|e|e.to_string())?;
            benchmark::write_new(&d.join("diamond.json"),&fixtures::diamond())?;
            benchmark::write_new(&d.join("chain-12.json"),&fixtures::chain(12))?;
            benchmark::write_new(&d.join("chain-24.json"),&fixtures::chain(24))?;
            benchmark::write_new(&d.join("wide-tree-320.json"),&fixtures::wide_tree(320))?;
            for seed in 0..12 {benchmark::write_new(&d.join(format!("random-{seed}.json")),&fixtures::random(seed,12))?;}
            for seed in 1000..1012 {benchmark::write_new(&d.join(format!("heldout-{seed}.json")),&fixtures::random(seed,24))?;}
            Ok(())
        },
        Some("smoke") if a.len()==3=>{
            let d=Path::new(&a[2]);fs::create_dir_all(d).map_err(|e|e.to_string())?;
            let code=benchmark::file_digest(&env::current_exe().map_err(|e|e.to_string())?)?;
            let mut cases=vec![("diamond".to_string(),fixtures::diamond(),3),("chain-12".to_string(),fixtures::chain(12),13),("chain-24".to_string(),fixtures::chain(24),25)];
            for seed in 0..4 {cases.push((format!("random-{seed}"),fixtures::random(seed,12),5));}
            let mut summary=Vec::new();
            for (name,snap,budget) in cases {
                let mut cfg=Config::default();cfg.budget=budget;cfg.neighborhood.width=20;
                let r=benchmark::run(&snap,&cfg,&d.join(&name),&code)?;
                summary.push(serde_json::json!({"case":name,"identity":r.identity,"cells":r.cells.len(),
                    "results":r.cells.iter().map(|c|serde_json::json!({"method":c.name,"value":c.outcome.value,"status":c.outcome.status,"seconds":c.incremental_wall_seconds})).collect::<Vec<_>>() }));
            }
            println!("{}",serde_json::to_string_pretty(&summary).map_err(|e|e.to_string())?);Ok(())
        },
        Some("run") if a.len()==5=>{
            let cfg:Config=benchmark::read_json(Path::new(&a[4]))?;cfg.validate()?;
            if fs::metadata(&a[2]).map_err(|e|e.to_string())?.len()>cfg.max_input_bytes {return Err("snapshot byte limit".into());}
            let snap:Snapshot=benchmark::read_json(Path::new(&a[2]))?;
            let code=benchmark::file_digest(&env::current_exe().map_err(|e|e.to_string())?)?;
            let r=benchmark::run(&snap,&cfg,Path::new(&a[3]),&code)?;
            println!("{}",serde_json::to_string_pretty(&r).map_err(|e|e.to_string())?);Ok(())
        },
        _=>Err("usage: optimizer-bench config FILE | plan FILE | fixtures DIR | smoke OUTPUT_DIR | run SNAPSHOT OUTPUT_DIR CONFIG".into()),
    }
}
fn main() {
    if let Err(e) = execute() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
