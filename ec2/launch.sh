#You need pip, fab and boto3 to run this script

command -v pip >/dev/null 2>&1 || { echo >&2 "pip is required to run this script."; exit 1; }
command -v fab >/dev/null 2>&1 || { echo >&2 "fab is required to run this script."; exit 1; }
python -c "import boto3" >/dev/null 2>&1 || { echo >&2 "boto3 is required to run this script."; exit 1; }

if [ "$#" -ne 0 ]; then
  fab -f launch.py $@
else
  fab -f launch.py launch
  fab -f launch.py savanna_cluster_setup
  fab -f launch.py savanna_master_setup
fi


