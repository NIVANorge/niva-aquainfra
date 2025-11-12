import os
import subprocess
import logging
LOGGER = logging.getLogger(__name__)

def run_docker_container(
        docker_executable,
        image_name,
        script_name,
        random_string,
        download_dir,
        inputs_read_only,
        script_args
    ):
    LOGGER.debug('Prepare running docker container (image %s)' % image_name)

    # Create container name
    # Note: Only [a-zA-Z0-9][a-zA-Z0-9_.-] are allowed
    #container_name = "%s_%s" % (image_name.split(':')[0], os.urandom(5).hex())
    container_name = "%s_%s" % (image_name.split(':')[0], random_string)

    # Define paths inside the container
    container_out = '/out'
    container_in_readonly = '/readonly'

    # Define local paths
    local_out = os.path.join(download_dir, "out")

    # Ensure directories exist
    os.makedirs(local_out, exist_ok=True)

    # Replace paths in args:
    sanitized_args = []
    LOGGER.debug('Args before sanitizing: %s' % script_args)
    for arg in script_args:
        newarg = arg
        if arg is None:
            newarg = 'null'
            LOGGER.debug("Replaced argument %s by %s..." % (arg, newarg))
        elif type(arg) == type(True):
            LOGGER.debug('Found a boolean: %s' % arg)
            if arg == True:
                newarg = 'true'
            elif arg == False:
                newarg = 'false'
            LOGGER.debug("Replaced argument %s by %s..." % (arg, newarg))
        elif inputs_read_only in arg:
            newarg = arg.replace(inputs_read_only, container_in_readonly)
            LOGGER.debug("Replaced argument %s by %s..." % (arg, newarg))
        elif local_out in arg:
            newarg = arg.replace(local_out, container_out)
            LOGGER.debug("Replaced argument %s by %s..." % (arg, newarg))
        sanitized_args.append(newarg)

    # Prepare container command
    # (mount volumes etc.)
    docker_args = [
        docker_executable, "run",
        "--rm",
        "--name", container_name,
        "-v", f"{inputs_read_only}:{container_in_readonly}",
        "-v", f"{local_out}:{container_out}",
        "-e", f"SCRIPT={script_name}",
        image_name,
    ]
    docker_command = docker_args + sanitized_args
    LOGGER.debug('Docker command: %s' % docker_command)
    
    # Run container
    try:
        LOGGER.debug('Start running docker container (image %s)' % image_name)
        result = subprocess.run(docker_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = result.stdout.decode()
        stderr = result.stderr.decode()
        LOGGER.debug('Finished running docker container (image %s)' % image_name)
        log_all_docker_output(stdout, stderr)
        return result.returncode, stdout, stderr, "no error"

    except subprocess.CalledProcessError as e:
        returncode = e.returncode
        stdout = e.stdout.decode()
        stderr = e.stderr.decode()
        LOGGER.error('Failed running docker container (exit code %s)' % returncode)
        user_err_msg = get_error_message_from_docker_stderr(stderr)
        return returncode, stdout, stderr, user_err_msg


def run_docker_container2(
        docker_executable,
        image_name,
        script_name,
        input_dir_on_host,
        output_dir_on_host,
        readonly_dir_on_host,
        script_args
    ):

    # Create container name
    # Note: Only [a-zA-Z0-9][a-zA-Z0-9_.-] are allowed
    # TODO: Use job-id?
    container_name = "%s_%s" % (image_name.split(':')[0], os.urandom(5).hex())
    LOGGER.debug(f'Prepare running docker: image {image_name}, container: {container_name}')

    # Define paths inside the container
    container_out = '/out'
    container_in = '/in'
    container_readonly = '/readonly'
    LOGGER.debug('Mounted dirs /out,/in,/readonly, inside container:  %s, %s, %s' %
        (container_out, container_in, container_readonly))

    # Define paths outside the container
    host_out = output_dir_on_host
    host_in = input_dir_on_host
    host_readonly = readonly_dir_on_host
    LOGGER.debug('Mounted dirs /out,/in,/readonly, outside container: %s, %s, %s' %
        (host_out, host_in, host_readonly))

    # Make sure no trailing slash:
    host_out      = host_out.rstrip("/")      if host_out else None
    host_in       = host_in.rstrip("/")       if host_in  else None
    host_readonly = host_readonly.rstrip("/") if host_readonly else None

    # Sanitize arguments passed to container!
    # i.e.: Replace host file paths by mounted file paths, convert args to formats
    # that can be passed to docker and understood/parsed in the R script inside docker:
    LOGGER.debug('Script args: %s' % script_args)
    sanitized_args = []
    for arg in script_args:
        newarg = arg
        if arg is None or arg == 'None':
            # R scripts may be more familiar with receiving "null" than "None"
            # But they still have to parse them to a proper NULL data type.
            newarg = 'null'
        elif isinstance(arg, bool):
            newarg = "true" if arg else "false"
            #LOGGER.debug(f'Arg: {arg}, type {type(arg)}, newarg {newarg}, type {type(newarg)}...)')
        elif host_in is not None and host_in in arg:
            newarg = arg.replace(host_in, container_in)
            LOGGER.debug("Replaced argument %s by %s..." % (arg, newarg))
        elif host_out is not None and host_out in arg:
            newarg = arg.replace(host_out, container_out)
            LOGGER.debug("Replaced argument %s by %s..." % (arg, newarg))
        elif host_readonly is not None and host_readonly in arg:
            newarg = arg.replace(host_readonly, container_readonly)
            LOGGER.debug("Replaced argument %s by %s..." % (arg, newarg))
        sanitized_args.append(newarg)

    # Prepare container command
    docker_args = [
        docker_executable, "run", "--rm",
        "--name", container_name
    ]
    # Add the mounts for three directories (-v) (ro and rw):
    if host_out is not None:
        docker_args = docker_args + ["-v", f"{host_out}:{container_out}:rw"]
    if host_in is not None:
        docker_args = docker_args + ["-v", f"{host_in}:{container_in}:rw"]
    if host_readonly is not None:
        docker_args = docker_args + ["-v", f"{host_readonly}:{container_readonly}:ro"]
    # Add the name of the script to be called (-e), and the name of the image
    docker_args = docker_args + [
        "-e", f"SCRIPT={script_name}",
        image_name
    ]
    # Add the arguments to be passed to the R script:
    docker_command = docker_args + sanitized_args
    LOGGER.debug('Docker command: %s' % docker_command)

    # Run container
    try:
        LOGGER.debug('Start running docker container')
        result = subprocess.run(docker_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = result.stdout.decode()
        stderr = result.stderr.decode()
        LOGGER.debug('Finished running docker container')

        # Print docker output:
        LOGGER.debug('DOCKER OUTPUT:')
        log_all_docker_output(stdout, stderr)

        return result.returncode, stdout, stderr, "no error"

    except subprocess.CalledProcessError as e:
        returncode = e.returncode
        stdout = e.stdout.decode()
        stderr = e.stderr.decode()
        LOGGER.error('Failed running docker container (exit code %s)' % returncode)
        log_all_docker_output(stdout, stderr)
        user_err_msg = get_error_message_from_docker_stderr(stderr)
        return returncode, stdout, stderr, user_err_msg




def run_docker_container3(
        docker_executable,
        image_name,
        script_name,
        output_dir_on_host,
        script_args
    ):
    # Same as run_docker_container2, but simplified: Only output directory is passed!

    # Create container name
    # Note: Only [a-zA-Z0-9][a-zA-Z0-9_.-] are allowed
    # TODO: Use job-id?
    container_name = "%s_%s" % (image_name.split(':')[0], os.urandom(5).hex())
    LOGGER.debug(f'Prepare running docker: image {image_name}, container: {container_name}')

    # Define paths inside the container
    container_out = '/out'
    LOGGER.debug('Mounted dir /out inside container:  %s' % container_out)

    # Define paths outside the container
    host_out = output_dir_on_host
    LOGGER.debug('Mounted dir /out outside container: %s' % host_out)

    # Make sure no trailing slash:
    host_out      = host_out.rstrip("/")      if host_out else None

    # Sanitize arguments passed to container!
    # i.e.: Replace host file paths by mounted file paths, convert args to formats
    # that can be passed to docker and understood/parsed in the R script inside docker:
    LOGGER.debug('Script args (before sanitizing): %s' % script_args)
    sanitized_args = []
    for arg in script_args:
        newarg = arg
        if arg is None or arg == 'None':
            # R scripts may be more familiar with receiving "null" than "None"
            # But they still have to parse them to a proper NULL data type.
            newarg = 'null'
        elif isinstance(arg, bool):
            newarg = "true" if arg else "false"
            #LOGGER.debug(f'Arg: {arg}, type {type(arg)}, newarg {newarg}, type {type(newarg)}...)')
        elif isinstance(arg, str) and host_out is not None and host_out in arg:
            # If arg is float, "host_out in arg" causes: TypeError: argument of type 'float' is not iterable
            newarg = arg.replace(host_out, container_out)
            LOGGER.debug("Replaced argument %s by %s..." % (arg, newarg))
        sanitized_args.append(newarg)
    
    LOGGER.debug('Script args (after sanitizing): %s' % sanitized_args)

    # Prepare container command
    docker_args = [
        docker_executable, "run", "--rm",
        "--name", container_name
    ]

    # Add the mounts for three directories (-v) (ro and rw):
    if host_out is not None:
        docker_args = docker_args + ["-v", f"{host_out}:{container_out}:rw"]

    # Add the name of the script to be called (-e), and the name of the image
    docker_args = docker_args + [
        "-e", f"SCRIPT={script_name}",
        image_name
    ]

    # Add the arguments to be passed to the R script:
    docker_command = docker_args + sanitized_args
    LOGGER.debug('Docker command: %s' % docker_command)

    # Run container
    try:
        LOGGER.debug('Start running docker container')
        result = subprocess.run(docker_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = result.stdout.decode()
        stderr = result.stderr.decode()
        LOGGER.debug('Finished running docker container')

        # Print docker output:
        LOGGER.debug('DOCKER OUTPUT:')
        log_all_docker_output(stdout, stderr)

        return result.returncode, stdout, stderr, "no error"

    except subprocess.CalledProcessError as e:
        returncode = e.returncode
        stdout = e.stdout.decode()
        stderr = e.stderr.decode()
        LOGGER.error('Failed running docker container (exit code %s)' % returncode)
        log_all_docker_output(stdout, stderr)
        user_err_msg = get_error_message_from_docker_stderr(stderr)
        return returncode, stdout, stderr, user_err_msg



def log_all_docker_output(stdout, stderr):

    for line in stdout.split('\n'):
        if line:
            LOGGER.debug('Docker stdout: %s' % line)
            # output of print() in R-script

    for line in stderr.split('\n'):
        if line:
            LOGGER.debug('Docker stderr: %s' % line)
            # output of message() in R-script


def get_error_message_from_docker_stderr(stderr, log_all_lines = True):
    '''
    We would like to return meaningful messages to users. For example, by
    printing ALL stderr lines, we get the following:

    ERROR - Docker stderr: Error in if (zz[which.max(zz)] < minpts) stop("All species do not have enough data after removing missing values and duplicates.") : 
    ERROR - Docker stderr:   argument is of length zero
    ERROR - Docker stderr: Calls: pred_extract
    ERROR - Docker stderr: Execution halted

    ERROR - Docker stderr: Error in pred_extract(data = speciesfiltered, raster = worldclim, lat = in_colname_lat,  : 
    ERROR - Docker stderr:   All species do not have enough data after removing missing values and duplicates.
    ERROR - Docker stderr: Execution halted

    Now, how to capture the meaningful part of that, which we want to return
    to the user? Here is a first attempt:
    '''

    user_err_msg = ""
    error_on_previous_line = False
    colon_on_previous_line = False
    for line in stderr.split('\n'):

        # Skip empty lines:
        if not line:
            continue

        # Print all non-empty lines to log:
        if log_all_lines:
            LOGGER.error('Docker stderr: %s' % line)

        # R error messages may start with the word "Error"
        if line.startswith("Error") or line.startswith("Fatal error"):
            #LOGGER.debug('### Found explicit error line: %s' % line.strip())
            user_err_msg += line.strip()
            error_on_previous_line = True

        # When R error messages are continued on another line, they may be
        # indented by two spaces.
        elif line.startswith("  ") and error_on_previous_line:
            #LOGGER.debug('### Found indented line following an error: %s' % line.strip())
            user_err_msg += " "+line.strip()
            error_on_previous_line = True

        # When R error messages end with a colon, they will be continued on
        # the next line, independently of their indentation I guess!
        elif colon_on_previous_line:
            #LOGGER.debug('### Found line following a colon: %s' % line.strip())
            user_err_msg += " "+line.strip()
            error_on_previous_line = True

        else:
            #LOGGER.debug('### Do not pass back to user: %s' % line.strip())
            error_on_previous_line = False

        # Remember whether this line ended with a colon, indicating that the
        # next line will continue with the error message:
        colon_on_previous_line = False
        if line.strip().endswith(":"):
            #LOGGER.debug('### Found a colon, next line will still be error!')
            colon_on_previous_line = True

    return user_err_msg

