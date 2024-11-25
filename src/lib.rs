pub mod component;
pub mod dag;

pub mod components {
    pub mod adder;
    pub mod channel_consumer;
    pub mod crash_test_dummy;
    pub mod flexible_wildcard_processor;
    pub mod long_running_task;
    pub mod multi_channel_consumer;
    pub mod string_length_counter;
    pub mod wildcard_processor;
}
