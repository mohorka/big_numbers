# Big numbers
Solving ~~1st task~~ all tasks for Big Data course.

## How to run 1st task?
Make sure you have poetry installed :)
then run `poetry install` inside the repo directory.

After that, you may run sctipts directly by names:

`generate_big_number -s desired_size -o output_file_name --batching`

`get_statistic -i input_file_name --allow_multithreading`

You may omit flags `batching` and `allow_multithreading` to remove their effects.

## How to run 4th task?
You need enviroment with `pyspark`. I used jupiter docker image, get it with:
`docker pull jupyter/pyspark-notebook:latest`
