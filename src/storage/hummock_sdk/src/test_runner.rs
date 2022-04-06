use std::cell::RefCell;
use std::env;

use super::test::*;
/// general tests function
pub fn run_general_test(cases: &[&TestDescAndFn]) {
    run_test_inner(cases, TestGeneralStates)
}
#[derive(Clone)]
struct TestGeneralStates;

pub trait TestHook {
    fn setup(&mut self);
    fn teardown(&mut self);
}

impl TestHook for TestGeneralStates {
    fn setup(&mut self) {}
    fn teardown(&mut self) {}
}

struct TestWatcher<T: TestHook> {
    name: String,
    hook: T,
}

impl<H: TestHook + 'static> TestWatcher<H> {
    fn new(name: String, mut hook: H) -> TestWatcher<H> {
        println!("test is runner,{}", name);
        hook.setup();
        TestWatcher { name, hook }
    }
}

impl<H: TestHook> Drop for TestWatcher<H> {
    fn drop(&mut self) {
        self.hook.teardown();
        println!("test is drop,{}", self.name);
    }
}

pub fn run_test_inner(cases: &[&TestDescAndFn], hook: impl TestHook + 'static + Clone + Send) {
    let cases = cases
        .iter()
        .map(|case| {
            let name = case.desc.name.as_slice().to_owned();
            let h = hook.clone();
            let f = match case.testfn {
                TestFn::StaticTestFn(f) => TestFn::DynTestFn(Box::new(move || {
                    let _watcher = TestWatcher::new(name, h);
                    f();
                })),
                TestFn::StaticBenchFn(f) => TestFn::DynTestFn(Box::new(move || {
                    let _watcher = TestWatcher::new(name, h);
                    bench::run_once(f);
                })),
                ref f => panic!("unexpected testfn {:?}", f),
            };
            TestDescAndFn {
                desc: case.desc.clone(),
                testfn: f,
            }
        })
        .collect();
    let args = env::args().collect::<Vec<_>>();
    test_main(&args, cases, None)
}

thread_local!(static FS: RefCell<Option<fail::FailScenario<'static>>> = RefCell::new(None));
#[derive(Clone)]
struct FailPointHook;

impl TestHook for FailPointHook {
    fn setup(&mut self) {
        FS.with(|s| {
            s.borrow_mut().take();
            *s.borrow_mut() = Some(fail::FailScenario::setup())
        })
    }
    fn teardown(&mut self) {
        FS.with(|s| {
            s.borrow_mut().take();
        })
    }
}
pub fn run_failpont_tests(cases: &[&TestDescAndFn]) {
    let mut cases1 = vec![];
    let mut cases2 = vec![];
    cases.iter().for_each(|case| {
        if case.desc.name.as_slice().contains("test_failpoint") {
            cases1.push(*case);
        } else {
            cases2.push(*case);
        }
    });
    if !cases1.is_empty() {
        run_test_inner(cases1.as_slice(), FailPointHook);
    }
    if !cases2.is_empty() {
        run_general_test(cases2.as_slice());
    }
}
