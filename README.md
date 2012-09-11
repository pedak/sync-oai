# ResourceSync OAI-Adapter

The ResourceSync OAI-Adapter propagates changing via the resync /protocollinfrastructure.

A client is provided to synchronize a filesystem directory with the resources.

## Quick start

Make sure Python 2.7.1 is running on your system:

    python --version

Install the [Tornado](http://www.tornadoweb.org/) and [SleekXMPP](https://github.com/fritzy/SleekXMPP), [PyYAML](http://pyyaml.org/), and [APScheduler](http://packages.python.org/APScheduler/) libraries:

    sudo easy_install tornado
    sudo easy_install sleekxmpp    
    sudo easy_install PyYAML
    sudo easy_install apscheduler
    
Get the ResourceSync Simulator from [Github](http://www.github.com/behas/resync-simulator):

    git clone git://github.com/pedak/sync-oai.git
    
Run the source oai-adapter (with the default configuration in /config/default.yaml):
    
    chmod u+x oai-adapter
    ./oai-adapter

Run the resync client against the simulated source

    chmod u+x resync-client
    ./resync-client http://localhost:8888 /tmp/sim 

Terminate the source simulator:

    CTRL-C


## How to define parameterized use cases

Parameterized Use Cases can be defined by creating a [YAML](http://www.yaml.org/) configuration file (e.g., example.yaml) and defining a set of parameters:


source:
    name: ResourceSync OAI-Adapter
    endpoint: http://eprints.mminf.univie.ac.at/cgi/oai2
    max_runs: 100
    sleep_time: 5
        
Additional **inventory**, **publisher**, and **change memory** implementations
can be attached for simulation purposes. For instance, the following configuration attaches a change memory implemented in the DynamicChangeSet class.

    inventory:
        class: StaticSourceInventory
        max_sitemap_entries: 500
        interval: 15
        uri_path: sitemap.xml

    changememory:
        class: DynamicChangeSet
        uri_path: /changes

See the examples in the **/config** directory for further details.