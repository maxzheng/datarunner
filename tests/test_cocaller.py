from mock import Mock

from cocaller import Cocaller, ARC


def test_cocaller(capsys):
    class Load(ARC):
        def __repr__(self):
            return 'Load("dest")'

        def run(self, num):
            return num * 2

    class Finish(ARC):
        def run(self):
            print('finished')

    transform = Mock(name='transform', return_value=3)
    cc = Cocaller(lambda: print("started"),
                  etl=[lambda: 2, transform, Load()],
                  )
    result = cc.run()

    transform.assert_called_with(2)
    assert result == 6

    out, err = capsys.readouterr()
    assert """\
<lambda>
started
etl
  <lambda>
  >> transform
  >> Load("dest")
""" == out
