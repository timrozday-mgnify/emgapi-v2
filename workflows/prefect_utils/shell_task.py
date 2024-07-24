from prefect import task, get_run_logger
from prefect_shell import ShellOperation


@task(
    retries=2,
    task_run_name="run shell command {command}",
)
async def run_shell_command(command, env=None, workdir=None):
    cmd = ShellOperation(commands=[command], env=env, return_all=True, workdir=workdir)
    result = await cmd.run()
    logger = get_run_logger()
    # ShellOperation returns a list of outputs
    if result:
        stdout = result[0] if result[0] else "No stdout"
        stderr = result[1] if len(result) > 1 else "No stderr"
        logger.info(f"stdout: {stdout}")
        if len(result) > 1:
            logger.error(f"stderr: {stderr}")
        else:
            logger.info(stderr)
        return stdout, stderr
    else:
        logger.info("No result from ShellOperation")
        return None, None