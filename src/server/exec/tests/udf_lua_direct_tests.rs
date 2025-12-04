use crate::scripts::{ScriptRegistry, load_global_default_scripts};

fn call_num(reg: &ScriptRegistry, name: &str, args: &[serde_json::Value]) -> Option<f64> {
    let v = reg.call_function_json(name, args).unwrap();
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::Null => None,
        _ => None,
    }
}

#[test]
fn lua_direct_cosine_vec_functions() {
    let reg = ScriptRegistry::new().unwrap();
    // Load default scalar scripts from scripts/scalars directory
    load_global_default_scripts(&reg).unwrap();

    // cosine_sim([1,0],[1,0]) == 1
    let v = call_num(&reg, "cosine_sim", &[serde_json::json!("[1,0]"), serde_json::json!("[1,0]")]);
    assert!((v.unwrap() - 1.0).abs() < 1e-12);

    // cosine_sim orthogonal == 0
    let v = call_num(&reg, "cosine_sim", &[serde_json::json!("1,0"), serde_json::json!("0,1")]);
    assert!(v.unwrap().abs() < 1e-12);

    // cosine_sim zero vector -> NULL
    let v = call_num(&reg, "cosine_sim", &[serde_json::json!("0,0,0"), serde_json::json!("1,2,3")]);
    assert!(v.is_none());

    // vec_l2 3-4-5 triangle
    let v = call_num(&reg, "vec_l2", &[serde_json::json!("0,0"), serde_json::json!("3,4")]);
    assert!((v.unwrap() - 5.0).abs() < 1e-12);

    // vec_ip simple dot product
    let v = call_num(&reg, "vec_ip", &[serde_json::json!("1,2,3"), serde_json::json!("4,5,6")]);
    assert!((v.unwrap() - 32.0).abs() < 1e-12);

    // to_vec normalization and invalid
    let v = reg.call_function_json("to_vec", &[serde_json::json!("[1, 2, 3]")]).unwrap();
    assert_eq!(v, serde_json::json!("1,2,3"));
    let v = reg.call_function_json("to_vec", &[serde_json::json!("1, a, 3")]).unwrap();
    assert_eq!(v, serde_json::Value::Null);
}
