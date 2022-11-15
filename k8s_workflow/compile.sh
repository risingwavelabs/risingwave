
#!/bin/bash 

test=true
if [[ $test ]]; then 
  echo -e "#!/bin/bash \necho \"hi risingwave\"" > /risingwave/bin/risingwave ; chmod +x /risingwave/bin/risingwave
  echo -e  "#!/bin/bash \necho \"hi frontend\"" > /risingwave/bin/frontend  ; chmod +x /risingwave/bin/frontend 
  echo -e  "#!/bin/bash \necho \"hi compute-node\"" > /risingwave/bin/compute-node  ; chmod +x /risingwave/bin/compute-node 
  echo -e  "#!/bin/bash \necho \"hi meta-node\"" > /risingwave/bin/meta-node  ; chmod +x /risingwave/bin/meta-node 
  echo -e  "#!/bin/bash \necho \"hi compactor\"" > /risingwave/bin/compactor ; chmod +x /risingwave/bin/compactor
  exit 0
fi 

cargo build -p risingwave_cmd -p risingwave_cmd_all --release --features "static-link static-log-level" && \
  mv /risingwave/target/release/{frontend,compute-node,meta-node,compactor,risingwave} /risingwave/bin/ && \
  cargo clean

objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/risingwave && \
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/frontend && \
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/compute-node && \
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/meta-node && \
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/compactor
