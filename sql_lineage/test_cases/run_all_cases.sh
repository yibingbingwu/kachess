readonly script_dir=$(cd $(dirname "${BASH_SOURCE[0]}"); echo $PWD)

cd ${script_dir}
for case_file in 00*.sql
do
  ./test_case.sh $case_file
done
