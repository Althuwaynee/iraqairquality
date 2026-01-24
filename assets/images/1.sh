#!/bin/bash
# Super simple PNG to SVG conversion

OUTPUT_DIR="svg_output"
mkdir -p "$OUTPUT_DIR"

echo "Converting PNG files to SVG..."

for png in *.png; do
    if [ -f "$png" ]; then
        svg="${png%.png}.svg"
        echo "Processing: $png"
        
        # Create a simple SVG wrapper with embedded PNG
        cat > "$OUTPUT_DIR/$svg" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" 
     xmlns:xlink="http://www.w3.org/1999/xlink"
     width="500" height="500" viewBox="0 0 500 500">
  <image xlink:href="data:image/png;base64,$(base64 -w0 "$png")" 
         width="500" height="500"/>
</svg>
EOF
        
        echo "  Created: $svg"
    fi
done

echo "Done! Files saved in $OUTPUT_DIR/"
