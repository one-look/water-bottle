# Set environment variables

*PYTHONDONTWRITEBYTECODE=1*
    Prevents Python from writing .pyc files to disk. This keeps the Docker container clean and slightly reduces image size.

*PYTHONUNBUFFERED=1*
    Forces stdout and stderr to be unbuffered. This ensures application logs are sent immediately to the terminal/Docker logs without being stuck in a buffer if the app crashes.


# Install system dependencies
*install -y gcc* 
    Installs the GNU Compiler Collection. A suite of tools used to compile source code (like C or C++) into machine code that a computer can run.

    Why install GCC?
         Many Python packages are "wrappers" around C code. If the package doesn't have a pre-built "wheel" for your OS, Python must compile it from source using GCC during the pip install step.

*rm -rf /var/lib/apt/lists/*
    Deletes the temporary package lists created by update

    Why install then delete?
        1.  Install: 
            You need the package lists (apt-get update) to find and download gcc.
        
        2.  Delete: 
            Once gcc is installed, the list of available packages is just "dead weight" taking up disk space.
        
        3.  Result: 
            By deleting the lists in the same RUN command, Docker never saves that extra data into the image layer, resulting in a significantly smaller final image.
            
# Install Python dependencies

**--no-cache-dir**
    Prevents pip from saving a copy of the downloaded packages (.whl or .tar.gz files) inside the container. Since Docker images should be as small as possible, caching is unnecessary; you only need the installed library, not the installer file.

# Copy application code
**COPY . .**
    First Dot (Source): 
        Refers to the current directory on your host machine (your laptop/server) where the Dockerfile is located. It tells Docker to grab everything in that folder.

    Second Dot (Destination): 
        Refers to the current working directory inside the Docker image (defined earlier by WORKDIR /app).

    Basically, it means: "Copy everything from my local folder into the image's current folder."

# Copy start script
**chmod +x start.sh**
    Stands for "change mode + executable." It grants "execute" permissions to the file, allowing the system to run start.sh as a script/program rather than just reading it as a text file.

**Non-Root User Configuration**

    useradd --create-home --shell /bin/bash app: 
        Creates a new system user named "app" with its own home directory and a standard bash shell.

    chown -R app:app /app: 
        Changes the owner and group of the /app directory to the new "app" user. The -R makes it recursive (applies to all files and subfolders).

    USER app: 
        Switches the active user from root (the superuser) to app.

    Why do this? 
        It follows the Principle of Least Privilege. If your application is hacked, the attacker only has the limited permissions of the "app" user, preventing them from modifying the underlying system or accessing sensitive root-level files.