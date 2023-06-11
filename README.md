# Big numbers
Solving ~~1st task~~ all tasks for Big Data course.

## How to run 1st task?
Make sure you have poetry installed :)
then run `poetry install` inside the repo directory.

After that, you may run sctipts directly by names:

`generate_big_number -s desired_size -o output_file_name --batching`

`get_statistic -i input_file_name --allow_multithreading`

You may omit flags `batching` and `allow_multithreading` to remove their effects.

## How to run 2nd task?
You need enviroment with `pyspark` and also `golang` installed. For this task i used `github codespaces` with 4 cpu.

Run `poetry install` and after it you may start discover `task2.ipynb` inside `goroutines` sub-directory.

As you may have already figured out, `golang` solution is inside same sub-directory. You may run in with:

`go run main.go -input_file your_binary_file_with_big_ints`

## Where is 3rd task?
Please read `Task3.md` for short report.

## How to run 4th task?
You need enviroment with `pyspark`. I used jupiter docker image, get it with:
`docker pull jupyter/pyspark-notebook:latest`

## How to run 5th task?
First of all, you need a data. Run `poetry install` and after make some [tricks](https://www.kaggle.com/general/74235) 

run `kaggle datasets  download berkeleyearth/climate-change-earth-surface-temperature-data --unzip`. 

Now you may discover `task5.ipynb` and `scripts` sub-directory.

Run `get_average_temperature -i input_file -c desired_city -y desired_year` to get average temperature for city in a given year.

Run `pytest` to test main script functionality.

Run `poetry build` to get a wheel.

