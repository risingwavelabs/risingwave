use smallvec::SmallVec;

pub trait PlanTreeNodeV2 {
    type PlanRef;

    fn inputs(&self) -> SmallVec<[Self::PlanRef; 2]>;
    fn clone_with_inputs(&self, inputs: impl Iterator<Item = Self::PlanRef>) -> Self;
}

macro_rules! impl_plan_tree_node_v2_for_unary {
    ($node_type:ident, $input_feild:ident) => {
        impl<P: Clone> crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2
            for $node_type<P>
        {
            type PlanRef = P;

            fn inputs(&self) -> SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![self.$input_feild.clone()]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let mut new = self.clone();
                new.$input_feild = inputs.next().expect("expect exactly 1 input");
                assert!(inputs.next().is_none(), "expect exactly 1 input");
                new.clone()
            }
        }
    };
}

macro_rules! impl_plan_tree_node_v2_for_binary {
    ($node_type:ident, $first_input_feild:ident, $second_input_feild:ident) => {
        impl<P: Clone> crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2
            for $node_type<P>
        {
            type PlanRef = P;

            fn inputs(&self) -> SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![
                    self.$first_input_feild.clone(),
                    self.$second_input_feild.clone()
                ]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let mut new = self.clone();
                new.$first_input_feild = inputs.next().expect("expect exactly 2 input");
                new.$second_input_feild = inputs.next().expect("expect exactly 2 input");
                assert!(inputs.next().is_none(), "expect exactly 2 input");
                new.clone()
            }
        }
    };
}
