from typing import List

import httpx
from prefect import flow, get_run_logger, task


@task()
def get_stars(repo: str):
    logger = get_run_logger()
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazers_count"]
    logger.info(f"{repo} has {count} stars!")
    return count


@flow(name="GitHub Stars")
def github_stars(repos: List[str]):
    logger = get_run_logger()
    counts = []
    for repo in repos:
        stars = get_stars(repo)
        logger.info(f"{repo} has {stars} stars!")
        counts.append(stars)
    return counts


# run the flow!
if __name__ == "__main__":
    github_stars(["EBI-Metagenomics/emg-viral-pipeline"])
