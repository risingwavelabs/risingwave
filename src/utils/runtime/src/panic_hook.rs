/// Set panic hook to abort the process if we're not catching unwind, without losing the information
/// of stack trace and await-tree.
pub fn set_panic_hook() {
    std::panic::update_hook(|default_hook, info| {
        default_hook(info);

        if let Some(context) = await_tree::current_tree() {
            println!("\n\n*** await tree context of current task ***\n");
            println!("{}\n", context);
        }

        if !risingwave_common::util::panic::is_catching_unwind() {
            std::process::abort();
        }
    });
}
