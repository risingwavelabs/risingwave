
#!/bin/bash -x

# TODO: Use this script docker/Dockerfile. This way we do not have to copy paste this
# TODO: Introduce a cmd line interface to handle test flag
# TODO: -p risingwave_cm and -p risingwave_cmd_all seperate

usage() {
    {
        echo "This script compiles risingwave"
        echo ""
        echo "Usage:"
        echo "$0 [-t] [-c] [-o] [-h] [-m]"
        echo ""
        echo "-t    Test run. Will create hello world dummy binaries. Use this to test your build setup. Default is false"
        echo "-c    Run cargo clean. Default is false"
        echo "-o    Optimize. Compress debug section. Default is false"
        echo "-o    Move binaries to /risingwave/bin/{binaryname}. Default is false"
        echo "-p    Build risingwave_cmd and the specified package. By default this script builds risingwave_cmd AND risingwave_cmd_all"
        echo "-h    Show this help message"
    } 1>&2

    exit 1
}

c=false
t=false
o=false 
m=false
p=false

while getopts ":p:tcohm" o; do
    case "${o}" in
        t)
            t=true
            ;;
        c)
            c=true
            ;;
        o)
            o=true
            ;;
        m)
            m=true
            ;;
        p)
            p=${OPTARG}
            ;;
        h)
            usage
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

mkdir -p /risingwave/target/release

# Set this to true, if you want to test the build pipeline
if [[ $t == true ]]; then 
  echo "test run only"
  # TODO: Do this with the component loop as well
  echo -e "#!/bin/bash\necho \"hi risingwave\"" > /risingwave/target/release/risingwave ; chmod +x /risingwave/target/release/risingwave
  echo -e  "#!/bin/bash\necho \"hi frontend\"" > /risingwave/target/release/frontend  ; chmod +x /risingwave/target/release/frontend 
  echo -e  "#!/bin/bash\necho \"hi compute-node\"" > /risingwave/target/release/compute-node  ; chmod +x /risingwave/target/release/compute-node 
  echo -e  "#!/bin/bash\necho \"hi meta-node\"" > /risingwave/target/release/meta-node  ; chmod +x /risingwave/target/release/meta-node 
  echo -e  "#!/bin/bash\necho \"hi compactor\"" > /risingwave/target/release/compactor ; chmod +x /risingwave/target/release/compactor
  exit 0
fi


if [[ $p == false ]]; then 
  echo "building risingwave_cmd and risingwave_cmd_all..."
  cargo build -p risingwave_cmd -p risingwave_cmd_all --release --features "static-link static-log-level"
else
  echo "building ${p}..."
  cargo build -p ${p} -p risingwave_cmd --release --features "static-link static-log-level"
fi

echo -e "\tdone building"


components=(
  "risingwave"
  "compute-node"
  "meta-node"
  "frontend"
  "compactor"
)

if [[ $o == true ]]; then 
  echo "compressing debug section..." # disable this with a flag 
  for component in "${components[@]}"
  do
    echo -e "\tcompressing ${component}..."
    objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/${component}
  done
  echo -e "\tdone compressing"
fi 



if [[ $m == true ]]; then 
  echo "moving to /risingwave/bin ..." 
  for component in "${components[@]}"
  do
    echo -e "\tmoving ${component}..."
    mv /risingwave/target/release/${component} /risingwave/bin/
  done
  echo -e "\tdone moving"
fi

if [[ $c == true ]]; then 
  echo "cargo clean..."
  cargo clean
fi 

