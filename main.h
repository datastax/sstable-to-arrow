#ifndef MAIN_H_
#define MAIN_H_

#include <fstream>
#include <iostream>
#include "sstable_statistics.h"
#include "deserialization_helper.h"
#include "sstable.h"

void read_statistics(std::string);
void read_data(std::string);

#endif
