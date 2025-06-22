# Usage

Build the project first.
```
> make build

> ./cost help
```

## Cost Analysis
```
> ./cost analyze \
    --platform=snowflake \
    --profile=/path/to/my_profile.yaml \
    --duration=10 \
    --resource_type=warehouse
```

## List Resources
```
> ./cost resources --platform=snowflake --profile=/path/to/my_profile.yaml
```