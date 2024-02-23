run:
		docker-compose up
test:
		docker exec -w /home/glue_user/workspace/jupyter_workspace/borrowing_base_v3/ -it borrowing_base_glue_container bash resources/test/run_tests.sh
down:
		docker-compose down
clean:
		docker system prune -a
check:
		pip3 install tox
	    tox
