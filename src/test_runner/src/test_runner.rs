// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.
use std::cell::RefCell;
use std::env;

use crate::test::*;
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
                    f()
                })),
                TestFn::StaticBenchFn(f) => TestFn::DynTestFn(Box::new(move || {
                    let _watcher = TestWatcher::new(name, h);
                    bench::run_once(f)
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

#[derive(Clone)]
struct SyncPointHook;

impl TestHook for SyncPointHook {
    fn setup(&mut self) {
        sync_point::reset();
    }

    fn teardown(&mut self) {
        sync_point::reset();
    }
}

// End Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.
pub fn run_failpont_tests(cases: &[&TestDescAndFn]) {
    let mut cases1 = vec![];
    let mut cases2 = vec![];
    let mut cases3 = vec![];
    cases.iter().for_each(|case| {
        if case.desc.name.as_slice().contains("test_syncpoints") {
            // sync_point tests should specify #[serial], because sync_point lib doesn't implement
            // an implicit global lock to order tests like fail-rs.
            cases1.push(*case);
        } else if case.desc.name.as_slice().contains("test_failpoints") {
            cases2.push(*case);
        } else {
            cases3.push(*case);
        }
    });
    if !cases1.is_empty() {
        run_test_inner(cases1.as_slice(), SyncPointHook);
    }
    if !cases2.is_empty() {
        run_test_inner(cases2.as_slice(), FailPointHook);
    }
    if !cases3.is_empty() {
        run_general_test(cases3.as_slice());
    }
}
