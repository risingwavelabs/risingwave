use std::ops::Deref;

use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse};

pub async fn handle_explain_analyze_stream_job(
    handler_args: HandlerArgs,
    job_id: u32,
) -> Result<RwPgResponse> {
    // query meta for fragment graph (names only)
    let meta_client = handler_args.session.env().meta_client();
    // TODO(kwannoel): Only fetch the names, actor_ids and graph of the fragments
    let fragments = {
        let mut fragment_map = meta_client.list_table_fragments(&[job_id]).await?;
        let (fragment_job_id, table_fragment_info) = fragment_map.drain().next().unwrap();
        assert_eq!(fragment_job_id, job_id);
        table_fragment_info.fragments
    };
    todo!()
}
// fn fragments_to_graph(fragments: Vec<TableFragmentInfo>) -> FragmentGraph {
//     let mut graph = FragmentGraph::new();
//     for fragment in fragments {
//         let fragment_id = fragment.fragment_id;
//         let fragment_name = fragment.fragment_name;
//         let actor_id = fragment.actor_id;
//         let mut fragment_node = FragmentNode::new(fragment_id, fragment_name, actor_id);
//         for dep in fragment.dependencies {
//             fragment_node.add_dependency(dep);
//         }
//         graph.add_node(fragment_node);
//     }
//     graph
// }
