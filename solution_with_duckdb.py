import duckdb as db

from logger import logger
import time


class Solution(object):
    @staticmethod
    def solve():
        request = db.sql(
            "SELECT read_csv_auto.column0,"
            "ROUND(AVG(read_csv_auto.column1), 1),"
            "MIN(read_csv_auto.column1),"
            "MAX(read_csv_auto.column1)"
            "FROM read_csv_auto('measurements.txt')"
            "GROUP BY read_csv_auto.column0 "
            "ORDER BY read_csv_auto.column0"
        )
        result = request.fetchall()
        for city, mean, min, max in result:
            print(f"{city}: {mean}, {min}, {max}")


def main():
    logger.info("Starting the solution")
    start = time.time()
    Solution.solve()
    logger.info(f"Solution finished, {time.time() - start:} seconds elapsed")


if __name__ == "__main__":
    main()
