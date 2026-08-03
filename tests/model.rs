use attograph::model::GraphDef;

#[test]
fn version_is_stable_and_distinguishes_changes() {
    let mut a = GraphDef::new("g");
    a.add_node("x");
    a.add_edge("x", "y");
    let mut b = GraphDef::new("g");
    b.add_node("x");
    b.add_edge("x", "y");
    assert_eq!(a.version(), b.version());

    b.add_node("z");
    assert_ne!(a.version(), b.version());
}

#[test]
fn canonicalization_ignores_insertion_order() {
    let mut a = GraphDef::new("g");
    a.add_node("a");
    a.add_edge("a", "b");
    let mut b = GraphDef::new("g");
    b.add_edge("a", "b");
    b.add_node("a");
    assert_eq!(a.version(), b.version());
}

#[test]
fn validates_cycles_exactly_once() {
    let mut g = GraphDef::new("loop");
    for n in ["a", "b", "c"] {
        g.add_node(n);
    }
    g.add_edge("a", "b");
    g.add_edge("b", "c");
    g.add_edge("c", "a");
    assert!(g.validate().is_err());
}

#[test]
fn validates_unknown_edge_nodes() {
    let mut g = GraphDef::new("g");
    g.add_node("a");
    g.add_edge("a", "ghost");
    assert!(g.validate().is_err());
}

#[test]
fn rejects_colon_in_node_tag() {
    let mut g = GraphDef::new("g");
    g.add_node("a:b");
    assert!(g.validate().is_err());
}

#[test]
fn round_trips_through_json_string() {
    let mut g = GraphDef::new("pipeline");
    g.add_node("load");
    g.add_edge("load", "save");
    let s = serde_json::to_string(&g).unwrap();
    let back = GraphDef::from_str(&s).unwrap();
    assert_eq!(g, back);
    assert_eq!(g.start_nodes(), back.start_nodes());
    assert_eq!(g.end_nodes(), back.end_nodes());
}

#[test]
fn classifies_start_and_end_nodes() {
    let mut g = GraphDef::new("diamond");
    for n in ["a", "b", "c", "d"] {
        g.add_node(n);
    }
    g.add_edge("a", "b");
    g.add_edge("a", "c");
    g.add_edge("b", "d");
    g.add_edge("c", "d");
    assert_eq!(g.start_nodes(), vec!["a".to_string()]);
    assert_eq!(g.end_nodes(), vec!["d".to_string()]);
    assert_eq!(g.predecessors("d"), vec!["b".to_string(), "c".to_string()]);
}
