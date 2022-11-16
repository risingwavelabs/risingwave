
#!/bin/bash -x

# TODO: Use this script docker/Dockerfile. This way we do not have to copy paste this
# TODO: Introduce a cmd line interface to handle test flag
# TODO: -p risingwave_cm and -p risingwave_cmd_all seperate


# Set this to true, if you want to test the build pipeline
test=false
if [[ $test == true ]]; then 
  echo "test run only"
  # TODO: Do this with the component loop as well
  echo -e "#!/bin/bash\necho \"hi risingwave\"" > /risingwave/bin/risingwave ; chmod +x /risingwave/bin/risingwave
  echo -e  "#!/bin/bash\necho \"hi frontend\"" > /risingwave/bin/frontend  ; chmod +x /risingwave/bin/frontend 
  echo -e  "#!/bin/bash\necho \"hi compute-node\"" > /risingwave/bin/compute-node  ; chmod +x /risingwave/bin/compute-node 
  echo -e  "#!/bin/bash\necho \"hi meta-node\"" > /risingwave/bin/meta-node  ; chmod +x /risingwave/bin/meta-node 
  echo -e  "#!/bin/bash\necho \"hi compactor\"" > /risingwave/bin/compactor ; chmod +x /risingwave/bin/compactor
  exit 0
fi


mkdir -p /risingwave/target/release

echo "building risingwave_cmd and risingwave_cmd_all..."
cargo build -p risingwave_cmd -p risingwave_cmd_all --release --features "static-link static-log-level"
echo -e "\tdone building"


echo "moving to /risingwave/bin ..." 
components=(
  "risingwave"
  "compute-node"
  "meta-node"
  "frontend"
  "compactor"
)
for component in "${components[@]}"
do
  echo -e "\tmoving ${component}..."
  mv /risingwave/target/release/${component} /risingwave/bin/
done
echo -e "\tdone moving"

# TODO: Only clean if parameter is passed
# echo "cargo clean..."
# cargo clean

echo "compressing debug section..." # disable this with a flag 
for component in "${components[@]}"
do
  echo -e "\tcompressing ${component}..."
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/${component}
done
echo -e "\tdone compressing"



