from prefect import flow, task
from typing import List
import httpx


@task(log_prints=True)
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")
    return count


@flow(name="GitHub Stars")
def github_stars(repos: List[str]):
    counts = []
    for repo in repos:
        stars = get_stars(repo)
        counts.append(stars)
    return counts


# run the flow!
if __name__ == "__main__":
    github_stars(["EBI-Metagenomics/emg-viral-pipeline"])
