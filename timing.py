from time import time_ns

global start
start = time_ns()


def print_timing(section: str):
    global start
    print(f"{section:30}", ((time_ns() - start) // 1000000) / 1000, "s", ((time_ns() - start) // 1000000) / 1000 / 60, "m")
    start = time_ns()
