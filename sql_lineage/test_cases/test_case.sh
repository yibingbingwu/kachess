if [ -z $1 ];then
  echo "ERROR: Usage $0 test_case_file"
  exit 2
fi

readonly script_dir=$(cd $(dirname "${BASH_SOURCE[0]}"); echo $PWD)
readonly test_case=$1
readonly base_fn=$(basename $test_case)
readonly case_id=$(ls $base_fn | sed -e 's/^\([^_]*\)_.*/\1/g')


cd ${script_dir}
echo "Change dir to $script_dir"

./setup.sh
java -cp ../target/sql_lineage-1.0.0.jar \
  project.kachess.sql_lineage.ParseSingleScript \
  -i $test_case -s 1000 >> logs/output.from_${base_fn} 2>&1
retcd=$?
if [ $retcd -ne 0 ];then
  echo "${base_fn} FAILED to PARSE. See error in logs/output.from_${base_fn}"
  exit 1
fi

if [ -f ./validate_${case_id}.sql ];then
  if mysql -AB --skip-column-names -u root bingql < ./validate_${case_id}.sql | grep -q "^0" ;then
    echo "${base_fn} FAILED to pass validation. Run validate_${case_id}.sql to find out"
  else
    echo "        ${base_fn} OK"
  fi
else
  echo "Did not find validate_${case_id}.sql. Skipping"
fi
echo
