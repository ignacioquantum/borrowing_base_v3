run:
		docker-compose up
test:
		docker exec -w /home/glue_user/workspace/jupyter_workspace/borrowing_base_v3/ -it borrowing_base_glue_container bash resources/test/run_tests.sh
down:
		docker-compose down
clean:
		 docker system prune -a
check:
		pip3 install pycodestyle
		pip3 install pylint
		pycodestyle --ignore E501,W503,W504,W291,W293 src/glue
		pylint --disable=C0302,C0114,C0115,R0903,C0103,E1205,C0301,R0912,R0914,R0913,W1514,R0801,E0401,E0601,W0622,W0718 src/glue
		make test