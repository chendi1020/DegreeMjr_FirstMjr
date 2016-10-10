var data = ["data", "data1", "Option 3"];

var select = d3.select('#container')
  .append('select')
  	.attr('class','select')
    .on('change',onchange)

var options = select
  .selectAll('option')
	.data(data).enter()
	.append('option')
		.text(function (d) { return d; });

function onchange() {
	selectValue = d3.select('select').property('value')
	d3.select('#container')
		.append('p')
		.text(selectValue + ' is the last selected option.')
};