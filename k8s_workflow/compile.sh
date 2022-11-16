
#!/bin/bash -x

# TODO: Use this script docker/Dockerfile. This way we do not have to copy paste this
# TODO: Introduce a cmd line interface to handle test flag
# TODO: -p risingwave_cm and -p risingwave_cmd_all seperate


# Set this to true, if you want to test the build pipeline
test=false
if [[ $test == true ]]; then 
  echo "test run only"
  echo -e "#!/bin/bash\necho \"hi risingwave\"" > /risingwave/bin/risingwave ; chmod +x /risingwave/bin/risingwave
  echo -e  "#!/bin/bash\necho \"hi frontend\"" > /risingwave/bin/frontend  ; chmod +x /risingwave/bin/frontend 
  echo -e  "#!/bin/bash\necho \"hi compute-node\"" > /risingwave/bin/compute-node  ; chmod +x /risingwave/bin/compute-node 
  echo -e  "#!/bin/bash\necho \"hi meta-node\"" > /risingwave/bin/meta-node  ; chmod +x /risingwave/bin/meta-node 
  echo -e  "#!/bin/bash\necho \"hi compactor\"" > /risingwave/bin/compactor ; chmod +x /risingwave/bin/compactor
  exit 0
fi


mkdir -p /risingwave/target/release
# mkdir -p /risingwave/target/release/{frontend,compute-node,meta-node,compactor,risingwave} # TODO: remove this line?

echo "building risingwave_cmd and risingwave_cmd_all..."
cargo build -p risingwave_cmd -p risingwave_cmd_all --release --features "static-link static-log-level"
echo "done building"
echo "moving to /risingwave/bin ..." # TODO: do this with a loop and give debug output on each step
mv /risingwave/target/release/{frontend,compute-node,meta-node,compactor,risingwave} /risingwave/bin/
echo "done moving"

# TODO: Only clean if parameter is passed
# echo "cargo clean..."
# cargo clean

# TODO: do this with a loop and write debug output on every step
echo "compressing debug section..."
objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/risingwave
objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/frontend
objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/compute-node
echo "done compressing debug section of risingwave, frontend and compute node"
objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/meta-node
objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/compactor
echo "done compressing all debug sections"



