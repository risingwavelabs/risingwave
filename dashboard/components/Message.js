/*
 * Copyright 2022 Singularity Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
import React from 'react';
import { useState, useRef } from "react"
import { Snackbar, Alert } from "@mui/material";

export default React.forwardRef((props, messageRef) => {

  let vertical = props.vertical || "bottom";
  let horizontal = props.horizontal || "left";

  const [alertOpen, setAlertOpen] = useState(false);
  const [content, setContent] = useState("");
  const [severity, setSeverity] = useState("success");

  const onAlertClose = () => {
    setAlertOpen(false);
  }

  messageRef.current = {
    info: (content) => {
      setSeverity("error");
      setContent(content);
      setAlertOpen(true);
    },
    warning: (content) => {
      setSeverity("warning");
      setContent(content);
      setAlertOpen(true);
    },
    success: (content) => {
      setSeverity("success");
      setContent(content);
      setAlertOpen(true);
    },
    error: (content) => {
      setSeverity("error");
      setContent(content);
      setAlertOpen(true);
    }
  }

  return (
    <Snackbar open={alertOpen} autoHideDuration={6000} onClose={onAlertClose} anchorOrigin={{ vertical, horizontal }}> 
      <Alert onClose={onAlertClose} severity={severity} sx={{ width: '100%' }} >
        {content}
      </Alert>
    </Snackbar>
  );
})