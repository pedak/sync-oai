"""ResourceSync Resource Container object

Both Inventory and Change Set objects are collections of Resource-like
objects (Resource or ResourceChange) with additional metadata regarding 
capabilities and discovery information.

This is a superclass for the Inventory and ChangeSet classes which 
contains common functionality.
"""

class ResourceContainer(object):
    """Class containing resource-like objects

    Core functionality::
    - resources property that is the set/list of resources
    -- add() to add a resource-like object to self.resources
    -- iter() to get iterator over self.resource in appropriate order
    - capabilities property that is a dict of capabilities

    Derived classes may add extra functionality such as len() etc..
    However, any code designed to work with any ResourceContainer
    should use only the core functionality.
    """

    def __init__(self, resources=None, capabilities=None):
        self.resources=resources
        self.capabilities=(capabilities if (capabilities is not None) else {})

    def __iter__(self):
        """Iterator over all the resources in this inventory

        Baseline implementation use iterator given by resources property
        """
        return(iter(self.resources))

    def add(self, resource):
        """Add a resource or an iterable collection of resources to this container

        Must be implemented in derived class
        """
        raise NotImplemented("add() not implemented")

    def __str__(self):
        """Return string of all resources in order given by interator"""
        s = ''
        for resource in self:
            s += str(resource) + "\n"
        return(s)
