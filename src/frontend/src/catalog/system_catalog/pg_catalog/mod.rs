// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod pg_am;
pub mod pg_attrdef;
pub mod pg_attribute;
pub mod pg_auth_members;
pub mod pg_cast;
pub mod pg_class;
pub mod pg_collation;
pub mod pg_constraint;
pub mod pg_conversion;
pub mod pg_database;
mod pg_depend;
pub mod pg_description;
pub mod pg_enum;
pub mod pg_extension;
pub mod pg_index;
pub mod pg_indexes;
pub mod pg_inherits;
pub mod pg_keywords;
pub mod pg_locks;
pub mod pg_matviews;
pub mod pg_namespace;
pub mod pg_opclass;
pub mod pg_operator;
pub mod pg_proc;
pub mod pg_roles;
pub mod pg_settings;
mod pg_shadow;
pub mod pg_shdescription;
pub mod pg_stat_activity;
pub mod pg_tables;
pub mod pg_tablespace;
pub mod pg_type;
pub mod pg_user;
pub mod pg_views;

pub use pg_am::*;
pub use pg_attrdef::*;
pub use pg_attribute::*;
pub use pg_auth_members::*;
pub use pg_cast::*;
pub use pg_class::*;
pub use pg_collation::*;
pub use pg_constraint::*;
pub use pg_conversion::*;
pub use pg_database::*;
pub use pg_depend::*;
pub use pg_description::*;
pub use pg_enum::*;
pub use pg_extension::*;
pub use pg_index::*;
pub use pg_indexes::*;
pub use pg_inherits::*;
pub use pg_keywords::*;
pub use pg_locks::*;
pub use pg_matviews::*;
pub use pg_namespace::*;
pub use pg_opclass::*;
pub use pg_operator::*;
pub use pg_proc::*;
pub use pg_roles::*;
pub use pg_settings::*;
pub use pg_shadow::*;
pub use pg_shdescription::*;
pub use pg_stat_activity::*;
pub use pg_tables::*;
pub use pg_tablespace::*;
pub use pg_type::*;
pub use pg_user::*;
pub use pg_views::*;
