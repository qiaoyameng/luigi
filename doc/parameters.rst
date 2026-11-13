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

.. _canonical-task-identity:

Canonical task identity
^^^^^^^^^^^^^^^^^^^^^^^

By default Luigi builds a task id from the *serialized strings* of its
significant parameters. Two caveats follow from that representation:

* the serialized form of a :class:`~luigi.contrib` style dictionary parameter
  preserves insertion order, so the same key/value pairs built in a different
  order can yield different task ids;
* values are flattened to strings before hashing, so distinct native values
  such as ``1``, ``True`` and ``"1"`` collide when passed to an untyped
  :class:`~luigi.parameter.Parameter`.

Set ``canonical_task_identity`` in the ``[core]`` section (or pass
``--canonical-task-identity`` on the command line) to opt into the canonical
identity mode, which hashes the actual, normalized parameter values instead:

.. code:: ini

    [core]
    canonical_task_identity: true

In this mode:

* mappings are compared by content, never by insertion order, including when
  nested inside lists, tuples or other mappings;
* every value carries an explicit type tag, so ``1``, ``True``, ``1.0`` and
  ``"1"`` produce distinct task ids;
* the supported value domain is closed: ``None``, ``bool``, ``int``,
  ``float``, :class:`decimal.Decimal`, :class:`datetime.date`,
  :class:`datetime.datetime`, :class:`datetime.timedelta`, ``str``,
  :class:`enum.Enum`, ``list``/``tuple`` and mappings with string keys.
  Unsupported objects and circular references raise
  :class:`luigi.canonical.CanonicalEncodingError` instead of being coerced
  through ``repr()``;
* naive datetimes keep their wall-clock representation while timezone-aware
  datetimes are converted to UTC (suffixed with ``Z``); the two conventions
  never share an identity;
* insignificant and private parameters still do not participate in the
  identity.

Canonical ids are suffixed with ``-v2`` so they can never be mistaken for a
legacy id. The mode therefore stays opt-in and fully backward compatible:
existing scheduler state keyed by legacy ids is not silently merged with
canonical ids, and a task reconstructed through
:meth:`~luigi.task.Task.from_str_params` rebuilds the same id it was scheduled
with.

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
