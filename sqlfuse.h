/*
  Copyright (C) 2013, 2014 Movsunov A.N.
  
  This file is part of SQLFuse

  SQLFuse is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  SQLFuse is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with SQLFuse.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef SQLFUSE_H
#define SQLFUSE_H


// Типы объектов
#define SF_DIR 0x01
#define SF_REG 0x02


// Внутренние коды ошибок
#define EENULL 0x101
#define EENOTSUP 0x102
#define EEMEM 0x109
#define EELOGIN 0x110
#define EECONN 0x111
#define EEUSE 0x112
#define EEINIT 0x113
#define EEBUSY 0x114
#define EECMD 0x121
#define EEXEC 0x122
#define EERES 0x123
#define EEFULL 0x124
#define EENOTFOUND 0x221
#define EEPARSE 0x222


#endif
