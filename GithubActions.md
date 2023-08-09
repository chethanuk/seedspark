# Github Actions

Github actions is under `.github` folder

## Trigger other workflows actions

Trigger test matrix build

```yaml
- name: Emit pre_push event
  uses: mvasigh/dispatch-action@v1.1.6
  with:
    token: ${{ secrets.PERSONAL_ACCESS_TOKEN }}
    repo: "@"
    event_type: pre_push
    debug: 1
    message: |
      {
        "bump_semver": "${{ github.event.inputs.bumpsemver }}"
      }
```

## Wait for workflow actions

Wait for PrePublishToPyPi test matrix build
This step will retry until required check passes
and will fail the whole workflow if the check conclusion is not a success

```yaml
- name: Wait on tests
  # or lewagon/wait-on-check-action@master, but master is not guaranteed to be stable ATM
  uses: lewagon/wait-on-check-action@v0.2
  with:
    ref: master # can be commit SHA or tag too
    # Read: https://github.com/lewagon/wait-on-check-action#figruring-out-check-name
    check-name: pre_publish_pypi_tests # name of the existing check - omit to wait for all checks
    repo-token: ${{ secrets.GITHUB_TOKEN }}
    wait-interval: 3600 # seconds
```
