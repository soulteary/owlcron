#!/usr/bin/env bun

// Frontend Build Script for OwlCron Engine
// Adapted from Cronicle's bin/build.js
// Copies, symlinks and compresses files into the right locations.
// Can also compact & bundle JS/CSS together for distribution.

var fs = require('fs');
var path = require('path');
var util = require('util');
var async = require('async');

var Tools = require('pixl-tools');
var mkdirp = Tools.mkdirp;

var BuildTools = require('./build-tools.js');
var setup = require('./setup.json');

var mode = 'dist';
if (process.argv.length > 2) mode = process.argv[2];

var steps = setup.build.common || [];
if (setup.build[mode]) {
	steps = steps.concat( setup.build[mode] );
}

// chdir to the engine root dir (parent of htdocs-deps)
process.chdir( path.resolve( __dirname, '..' ) );

// make sure we have a logs dir
mkdirp.sync( 'logs' );
fs.chmodSync( 'logs', "755" );

// log to file instead of console
console.log = function(msg, data) {
	if (data) msg += ' ' + JSON.stringify(data);
	fs.appendFile( 'logs/install.log', msg + "\n", function() {} );
	if (mode == 'dev') process.stdout.write( msg + "\n" );
};

console.log("\nBuilding project ("+mode+")...\n");

async.eachSeries( steps, function(step, callback) {
	// foreach step
	var isArray = Array.isArray || util.isArray;
	
	if (isArray(step)) {
		var func = step.shift();
		console.log( func + ": " + JSON.stringify(step));
		step.push( callback );
		BuildTools[func].apply( null, step );
	}
	else {
		var func = step.action;
		delete step.action;
		console.log( func + ": " + JSON.stringify(step));
		BuildTools[func].apply( null, [step, callback] );
	}
}, 
function(err) {
	if (err) {
		process.exitCode = 1;
		console.error("\nBuild Error: " + err + "\n");
	}
	else {
		console.log("\nBuild complete.\n");
	}
} );
