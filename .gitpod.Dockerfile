FROM gitpod/workspace-python-3.12

USER gitpod

RUN python3 -m pip install --user pipx && \
    python3 -m pipx ensurepath && \
    python3 -m pipx install invoke && \
    invoke --print-completion-script=bash >> $HOME/.bash_completion

# Poetry is already installed in the base Gitpod Python image,
# but we need to upgrade it
RUN poetry self update && \
    poetry completions bash >> ~/.bash_completion
