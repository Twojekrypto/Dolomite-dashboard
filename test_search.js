const fs = require('fs');
const html = fs.readFileSync('index.html', 'utf8');

// basic regex extraction to see what dolo_applyHolderFilters looks like
const match = html.match(/dolo_applyHolderFilters\(\) \{([\s\S]*?)\}/);
if(match) {
    console.log("Found function:", match[0].substring(0, 500));
} else {
    console.log("Function not found");
}
