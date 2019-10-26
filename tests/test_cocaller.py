from mock import Mock

from cocaller import Workflow, Step


def test_arc():
    class Lightning(Step):
        def run(self):
            return [1, 2, 3]

    arc = Lightning()
    assert arc() == [1, 2, 3]       # Callable
    assert arc.run() == [1, 2, 3]   # Runnable
    assert list(arc) == [1, 2, 3]   # Iterable


def test_cocaller(capsys):
    class Load(Step):
        def __repr__(self):
            return 'Load("dest")'

        def run(self, num):
            return num * 2

    class Finish(Step):
        def run(self):
            print('finished')

    transform = Mock(name='transform', return_value=3)
    cc = Workflow(lambda: print("started"),
                  [lambda: 1, transform, Load()],
                  etl=[lambda: 2, transform, Load()],
                  )
    result = cc.run()

    transform.assert_called_with(2)
    assert result == 6

    out, err = capsys.readouterr()
    print(out)
    assert """\
<lambda>
started

<lambda>
>> transform
>> Load("dest")

etl
  <lambda>
  >> transform
  >> Load("dest")
""" == out


def test_guess_name():
    assert Workflow._guess_name(lambda: 1) == '<lambda>'
    assert Workflow._guess_name(print) == 'print'
    assert Workflow._guess_name(Mock(name='mocked')) == 'mocked'

    class Step1(Step):
        pass
    assert Workflow._guess_name(Step1()) == 'Step1'

    class Step2(Step):
        def __str__(self):
            return 'Super Step'
    assert Workflow._guess_name(Step2()) == 'Super Step'

    class Step3(Step):
        name = "Fast Step"
    assert Workflow._guess_name(Step3()) == 'Fast Step'
