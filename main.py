from logger import logger
import time
import dask.dataframe as dd
class Solution(object):
    """Решение задачи с помощью библиотеки Dask"""
    @staticmethod
    def solve():
        df = dd.read_csv('measurements.txt', delimiter=';', names=["city", "temperature"])
        result = df.groupby('city').temperature.agg(
            mean='mean',
            max='max',
            min='min',
        ).sort_values('city').round(1).compute()
        for i in result.index:
            print(f"{i}: {result.loc[i, 'mean']}, {result.loc[i, 'min']}, {result.loc[i, 'max']}")


def main():
    logger.info("Starting the solution")
    start = time.time()
    Solution.solve()
    logger.info(f"Solution finished, {time.time() - start:} seconds elapsed")


if __name__ == "__main__":
    main()
