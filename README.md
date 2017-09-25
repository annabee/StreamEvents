
### Running Instructions
This is a Spark application and as such it depends on a spark cluster. I used [docker](https://hub.docker.com/r/p7hb/docker-spark/)

If using the above, start a docker image as such :
`docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -h spark --name=spark p7hb/docker-spark:2.1.0`


#### Producer Application
To run the application that produces events, you need to supply four program arguments to it.


`sbt run --number-of-orders=10 --batch-size=5 --interval=1 --output-directory="~/code/StreamEvents/output"`