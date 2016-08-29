\sdk\swig\swig -c++ -python -shadow -I\sdk\swig\swig_lib -I..\..\include _PyOOSQL.i
move _PyOOSQL_wrap.c _PyOOSQL_wrap.cxx
copy _PyOOSQL.py ..\..\..\bin
copy PyOOSQL.py ..\..\..\bin
