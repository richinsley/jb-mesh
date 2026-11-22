# Tool configuration

A tool can declare configuration in its manifest. The node validates the values, keeps them, and hands them to the tool's process when it starts.

## Declaring it

```yaml
config:
  required: [data_dir]
  schema:
    data_dir:
      type: string
    batch:
      type: int
      default: 8
    mode:
      type: string
      enum: [fast, careful]
```

Each key has a `type` of `string`, `int`, `float` or `bool`, and may have a `default` and an `enum` of allowed values. A key listed under `required` must end up with a value. `jb-mesh preflight` checks that every required key is in the schema.

A manifest with no `config` section accepts any values.

## Where the values come from

The effective configuration is the schema's defaults with the tool's saved values on top. Saved values are in `config.json` beside the tool's installed `jumpboot.yaml`. They are written by a config request to the node (`Mesh.RequestConfig` with the action `set`), which validates before it saves, so a refused request changes nothing. A `set` replaces the saved values as a whole.

The tool's process receives the result as JSON in the `JB_TOOL_CONFIG` environment variable. The variable is absent when there is nothing to pass: no defaults and no saved values.

## An invalid configuration stops the start

The configuration is checked again every time a tool process is made. It is invalid when:

- `config.json` cannot be read or is not JSON
- it has a key the schema does not declare
- a value has the wrong type or is outside its `enum`
- a required key has no value

A tool with an invalid configuration is **not started**, and the error says which of these it was. For a one-shot tool the call fails with that error. A persistent tool is not started and not registered, so it does not answer on the mesh; the node logs why, and an install reports it.

Up to v0.1.6 the error was discarded and the tool was started with no `JB_TOOL_CONFIG` at all, declared defaults included. A service that takes a data location or an address from its configuration then ran on whatever it assumes when given none.

To bring a refused tool up, set a valid configuration. Setting the configuration of a persistent tool restarts it if it is running, and starts and registers it if it is not.

If that start fails for another reason, the tool is left with no route on the mesh: its service, its node-targeted subjects and its streaming subjects are all withdrawn, and a reconnect does not restore them.

Two cases to expect:

- **A persistent tool with a required key and no default** is refused when first installed. Set its configuration and it starts.
- **An update whose manifest drops a key** that is still in `config.json` is refused at start. Set the configuration again without that key.
