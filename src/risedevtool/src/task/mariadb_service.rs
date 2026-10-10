// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use super::docker_service::{DockerService, DockerServiceConfig};
use crate::MariaDbConfig;

impl DockerServiceConfig for MariaDbConfig {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn is_user_managed(&self) -> bool {
        self.user_managed
    }

    fn image(&self) -> String {
        self.image.clone()
    }

    fn args(&self) -> Vec<String> {
        vec![
            "--log-bin=mariadb-bin".to_owned(),
            "--binlog-format=ROW".to_owned(),
            "--binlog-row-image=FULL".to_owned(),
            "--server-id=4101".to_owned(),
            "--binlog-legacy-event-pos=ON".to_owned(),
        ]
    }

    fn envs(&self) -> Vec<(String, String)> {
        let mut envs = vec![
            ("MARIADB_DATABASE".to_owned(), self.database.clone()),
            ("MARIADB_ROOT_HOST".to_owned(), "%".to_owned()),
        ];
        if self.user == "root" {
            if self.password.is_empty() {
                envs.push((
                    "MARIADB_ALLOW_EMPTY_ROOT_PASSWORD".to_owned(),
                    "1".to_owned(),
                ));
            } else {
                envs.push(("MARIADB_ROOT_PASSWORD".to_owned(), self.password.clone()));
            }
        } else {
            envs.extend([
                (
                    "MARIADB_ALLOW_EMPTY_ROOT_PASSWORD".to_owned(),
                    "1".to_owned(),
                ),
                ("MARIADB_USER".to_owned(), self.user.clone()),
                ("MARIADB_PASSWORD".to_owned(), self.password.clone()),
            ]);
        }
        envs
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![(self.port.to_string(), "3306".to_owned())]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data.then(|| "/var/lib/mysql".to_owned())
    }
}

pub type MariaDbService = DockerService<MariaDbConfig>;
