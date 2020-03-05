# number of processes to run the tests in
export PARALLEL_SPLIT_TEST_PROCESSES=15

bundle install
cd spec
rm -rf results
mkdir -p results/data
mkdir -p results/sql
mkdir -p results/reports
echo 'Hang in there, running tests, this might take a while'
parallel_split_test run_tests.rb --format html --out results/reports/results.html