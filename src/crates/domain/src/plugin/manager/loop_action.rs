/// What to do after handling one node's execution result inside the loop.
#[derive(Debug, PartialEq)]
pub(super) enum LoopAction {
    Advance,
    Retry,
    Done,
}
