Parameters
----------

Parameters is the Luigi equivalent of creating a constructor for each Task.
Luigi requires you to declare these parameters by instantiating
:class:`~luigi.parameter.Parameter` objects on the class scope:

.. code:: python

    class DailyReport(luigi.contrib.hadoop.JobTask):
        date = luigi.DateParameter(default=datetime.date.today())
        # ...

By doing this, Luigi can take care of all the boilerplate code that
would normally be needed in the constructor.
Internally, the DailyReport object can now be constructed by running
``DailyReport(datetime.date(2012, 5, 10))`` or just ``DailyReport()``.
Luigi also creates a command line parser that automatically handles the
conversion from strings to Python types.
This way you can invoke the job on the command line eg. by passing ``--date 2012-05-10``.

The parameters are all set to their values on the Task object instance,
i.e.

.. code:: python

    d = DailyReport(datetime.date(2012, 5, 10))
    print(d.date)

will return the same date that the object was constructed with.
Same goes if you invoke Luigi on the command line.

.. _Parameter-instance-caching:

Instance caching
^^^^^^^^^^^^^^^^

Tasks are uniquely identified by their class name and values of their
parameters.
In fact, within the same worker, two tasks of the same class with
parameters of the same values are not just equal, but the same instance:

.. code:: python

    >>> import luigi
    >>> import datetime
    >>> class DateTask(luigi.Task):
    ...   date = luigi.DateParameter()
    ...
    >>> a = datetime.date(2014, 1, 21)
    >>> b = datetime.date(2014, 1, 21)
    >>> a is b
    False
    >>> c = DateTask(date=a)
    >>> d = DateTask(date=b)
    >>> c
    DateTask(date=2014-01-21)
    >>> d
    DateTask(date=2014-01-21)
    >>> c is d
    True

Insignificant parameters
^^^^^^^^^^^^^^^^^^^^^^^^

If a parameter is created with ``significant=False``,
it is ignored as far as the Task signature is concerned.
Tasks created with only insignificant parameters differing have the same signature but
are not the same instance:

.. code:: python

    >>> class DateTask2(DateTask):
    ...   other = luigi.Parameter(significant=False)
    ...
    >>> c = DateTask2(date=a, other="foo")
    >>> d = DateTask2(date=b, other="bar")
    >>> c
    DateTask2(date=2014-01-21)
    >>> d
    DateTask2(date=2014-01-21)
    >>> c.other
    'foo'
    >>> d.other
    'bar'
    >>> c is d
    False
    >>> hash(c) == hash(d)
    True

Parameter visibility
^^^^^^^^^^^^^^^^^^^^

Using :class:`~luigi.parameter.ParameterVisibility` you can configure parameter visibility. By default, all
parameters are public, but you can also set them hidden or private.

.. code:: python

    >>> import luigi
    >>> from luigi.parameter import ParameterVisibility
    
    >>> luigi.Parameter(visibility=ParameterVisibility.PRIVATE)

``ParameterVisibility.PUBLIC`` (default) - visible everywhere

``ParameterVisibility.HIDDEN`` - ignored in WEB-view, but saved into database if save db_history is true

``ParameterVisibility.PRIVATE`` - visible only inside task.

Secret parameters
^^^^^^^^^^^^^^^^^

Some parameters are secrets -- access tokens, passwords or API keys that
must never be written into task ids, scheduler state, error pages or logs.
Use :class:`~luigi.SecretParameter` for those.  A secret parameter carries
only a *reference* of the form ``"<name>@<version>"``; the plaintext is
fetched on the worker, immediately before the task runs, through an explicit
:class:`~luigi.secrets.SecretResolver`:

.. code:: python

    import luigi
    from luigi.secrets import SecretResolver

    class EnvSecretResolver(SecretResolver):
        def resolve(self, reference):
            return os.environ[f"{reference.name}_{reference.version}"]

    class CallApi(luigi.Task):
        token = luigi.SecretParameter()  # passed as, e.g., "github-token@v3"

        def run(self):
            # self.token behaves like the plaintext string only while run() executes
            requests.get(..., headers={"Authorization": f"Bearer {self.token}"})

    luigi.build([CallApi(token="github-token@v3")],
                local_scheduler=True, secret_resolver=EnvSecretResolver())

Properties worth knowing:

* Only the reference is serialized, so the task id, scheduler RPC payloads,
  web views, failure messages and logs can never contain the plaintext.
* The version is part of the task identity: rotating a secret to a new
  version produces a distinct task instead of changing an existing task id.
* Every (re)try calls the resolver again.  If resolution fails, the task
  does not start; it fails with a reference-only explanation and is retried.
* A resolver can also be attached to the parameter itself
  (``SecretParameter(resolver=...)``) or looked up by name
  (``SecretParameter(resolver_name="vault")`` with
  ``secret_resolvers={"vault": ...}``).

Parameter types
^^^^^^^^^^^^^^^

In the examples above, the *type* of the parameter is determined by using different
subclasses of :class:`~luigi.parameter.Parameter`. There are a few of them, like
:class:`~luigi.parameter.DateParameter`,
:class:`~luigi.parameter.DateIntervalParameter`,
:class:`~luigi.parameter.IntParameter`,
:class:`~luigi.parameter.FloatParameter`, etc.

Python is not a statically typed language and you don't have to specify the types
of any of your parameters.
You can simply use the base class :class:`~luigi.parameter.Parameter` if you don't care.

The reason you would use a subclass like :class:`~luigi.parameter.DateParameter`
is that Luigi needs to know its type for the command line interaction.
That's how it knows how to convert a string provided on the command line to
the corresponding type (i.e. datetime.date instead of a string).

.. _Parameter-class-level-parameters:

Setting parameter value for other classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All parameters are also exposed on a class level on the command line interface.
For instance, say you have classes TaskA and TaskB:

.. code:: python

    class TaskA(luigi.Task):
        x = luigi.Parameter()

    class TaskB(luigi.Task):
        y = luigi.Parameter()


You can run ``TaskB`` on the command line: ``luigi TaskB --y 42``.
But you can also set the class value of ``TaskA`` by running
``luigi TaskB --y 42 --TaskA-x 43``.
This sets the value of ``TaskA.x`` to 43 on a *class* level.
It is still possible to override it inside Python if you instantiate ``TaskA(x=44)``.

All parameters can also be set from the configuration file.
For instance, you can put this in the config:

.. code:: ini

    [TaskA]
    x: 45


Just as in the previous case, this will set the value of ``TaskA.x`` to 45 on the *class* level.
And likewise, it is still possible to override it inside Python if you instantiate ``TaskA(x=44)``.

Parameter resolution order
^^^^^^^^^^^^^^^^^^^^^^^^^^

Parameters are resolved in the following order of decreasing priority:

1. Any value passed to the constructor, or task level value set on the command line (applies on an instance level)
2. Any value set on the command line (applies on a class level)
3. Any configuration option (applies on a class level)
4. Any default value provided to the parameter (applies on a class level)

See the :class:`~luigi.parameter.Parameter` class for more information.
