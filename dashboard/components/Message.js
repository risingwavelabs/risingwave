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