build: tema1

tema1: tema1.cpp
	g++ tema1.cpp -o tema1 -lpthread -Wall -Werror

.PHONY: clean
clean:
	rm -f tema1