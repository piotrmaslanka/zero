// Locate the starting block
int start_index = 0;
while (files.get(start_index) <= from) {
    if (start_index == files.size()-1)
        break; 
    // We cannot advance further. This index is the seeked-for
    // position

    if (files.get(start_index+1) <= from) start_index++;
    else break;
}
// start_index is the index in files() of the block containing 
// our starting datapoint. Locate the ending block
int end_index = start_index;
while (end_index < files.size()-1) { 
    // while we can do any advancing...
    if (files.get(end_index+1) <= to) end_index++;
    else break;
}