while (imax >= imin) {
    imid = imin + ((imax - imin) / 2);
    this.cfile.position(imid*(8+this.recsize));
    this.cfile.read(this.timestamp_buffer);
    this.timestamp_buffer.flip();
    temp = this.timestamp_buffer.getLong();
    this.timestamp_buffer.clear();
    if (temp == time)
        return imid;
    if (temp < time) {
        if (imid == amax) 
            // Range exhausted right-side
            if (is_start)
                throw new IllegalArgumentException();
            else
                return amax;
        imin = imid + 1;
    } else {
        if (imid == 0) 
            // Range exhausted left-side
            if (is_start)
                return 0;
            else
                throw new IllegalArgumentException();
        imax = imid - 1;
    }
}
// Still not found. Interpolate.
if (is_start) {
    if (temp < time)
        return imid+1;
    else
        return imid;
} else {
    if (temp < time)
        return imid;
    else
        return imid-1;
}