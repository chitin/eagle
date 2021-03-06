/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

(function() {
	'use strict';

	var featureControllers = angular.module("featureControllers");
	var feature = featureControllers.register("common");

	// ==============================================================
	// =                          Function                          =
	// ==============================================================
	feature.service("Policy", function(Entities) {
		var Policy = function () {};

		Policy.updatePolicyStatus = function(policy, status) {
			$.dialog({
				title: "Confirm",
				content: "Do you want to " + (status ? "enable" : "disable") + " policy[" + policy.tags.policyId + "]?",
				confirm: true
			}, function(ret) {
				if(ret) {
					policy.enabled = status;
					Entities.updateEntity("AlertDefinitionService", policy);
				}
			});
		};
		Policy.deletePolicy = function(policy, callback) {
			$.dialog({
				title: "Confirm",
				content: "Do you want to delete policy[" + policy.tags.policyId + "]?",
				confirm: true
			}, function(ret) {
				if(ret) {
					policy.enabled = status;
					Entities.deleteEntity("AlertDefinitionService", policy)._promise.finally(function() {
						if(callback) {
							callback(policy);
						}
					});
				}
			});
		};
		return Policy;
	});

	feature.service("Notification", function(Entities) {
		var Notification = function () {};
		Notification.map = {};

		Notification.list = Entities.queryEntities("AlertNotificationService");
		Notification.list._promise.then(function () {
			$.each(Notification.list, function (i, notification) {
				// Parse fields
				notification.fieldList = common.parseJSON(notification.fields, []);

				// Fill map
				Notification.map[notification.tags.notificationType] = notification;
			});
		});

		Notification.promise = Notification.list._promise;

		return Notification;
	});

	// ==============================================================
	// =                          Policies                          =
	// ==============================================================

	// ========================= Policy List ========================
	feature.navItem("policyList", "Policies", "list");
	feature.controller('policyList', function(PageConfig, Site, $scope, Application, Entities, Policy) {
		PageConfig.pageTitle = "Policy List";
		PageConfig.pageSubTitle = Site.current().tags.site;

		// Initial load
		$scope.policyList = [];
		$scope.application = Application.current();

		// List policies
		var _policyList = Entities.queryEntities("AlertDefinitionService", {site: Site.current().tags.site, application: $scope.application.tags.application});
		_policyList._promise.then(function() {
			$.each(_policyList, function(i, policy) {
				policy.__expression = common.parseJSON(policy.policyDef, {}).expression;

				$scope.policyList.push(policy);
			});
		});
		$scope.policyList._promise = _policyList._promise;

		// Function
		$scope.searchFunc = function(item) {
			var key = $scope.search;
			if(!key) return true;

			var _key = key.toLowerCase();
			function _hasKey(item, path) {
				var _value = common.getValueByPath(item, path, "").toLowerCase();
				return _value.indexOf(_key) !== -1;
			}
			return _hasKey(item, "tags.policyId") || _hasKey(item, "__expression") || _hasKey(item, "description") || _hasKey(item, "owner");
		};

		$scope.updatePolicyStatus = Policy.updatePolicyStatus;
		$scope.deletePolicy = function(policy) {
			Policy.deletePolicy(policy, function(policy) {
				var _index = $scope.policyList.indexOf(policy);
				$scope.policyList.splice(_index, 1);
			});
		};
	});

	// ======================= Policy Detail ========================
	feature.controller('policyDetail', function(PageConfig, Site, $scope, $wrapState, $interval, Entities, Policy, nvd3) {
		var MAX_PAGESIZE = 10000;
		var seriesRefreshInterval;

		PageConfig.pageTitle = "Policy Detail";
		PageConfig.lockSite = true;
		PageConfig
			.addNavPath("Policy List", "/common/policyList")
			.addNavPath("Policy Detail");

		$scope.chartConfig = {
			chart: "line",
			xType: "time"
		};

		// Query policy
		if($wrapState.param.filter) {
			$scope.policyList = Entities.queryEntity("AlertDefinitionService", $wrapState.param.filter);
		} else {
			$scope.policyList = Entities.queryEntities("AlertDefinitionService", {
				policyId: $wrapState.param.policy,
				site: $wrapState.param.site,
				alertExecutorId: $wrapState.param.executor
			});
		}

		$scope.policyList._promise.then(function() {
			var policy = null;

			if($scope.policyList.length === 0) {
				$.dialog({
					title: "OPS!",
					content: "Policy not found!"
				}, function() {
					location.href = "#/common/policyList";
				});
				return;
			} else {
				policy = $scope.policyList[0];

				policy.__notificationList = common.parseJSON(policy.notificationDef, []);

				policy.__expression = common.parseJSON(policy.policyDef, {}).expression;

				$scope.policy = policy;
				Site.current(Site.find($scope.policy.tags.site));
				console.log($scope.policy);
			}

			// Visualization
			var _intervalType = 0;
			var _intervalList = [
				["Daily", 1440, function() {
					var _endTime = app.time.now().hour(23).minute(59).second(59).millisecond(0);
					var _startTime = _endTime.clone().subtract(1, "month").hour(0).minute(0).second(0).millisecond(0);
					return [_startTime, _endTime];
				}],
				["Hourly", 60, function() {
					var _endTime = app.time.now().minute(59).second(59).millisecond(0);
					var _startTime = _endTime.clone().subtract(48, "hour").minute(0).second(0).millisecond(0);
					return [_startTime, _endTime];
				}],
				["Every 5 minutes", 5, function() {
					var _endTime = app.time.now().second(59).millisecond(0);
					var _minute = Math.floor(_endTime.minute() / 5) * 5;
					var _startTime = _endTime.clone().minute(_minute).subtract(5 * 30, "minute").second(0).millisecond(0);
					_endTime.minute(_minute + 4);
					return [_startTime, _endTime];
				}]
			];

			function _loadSeries(seriesName, metricName, condition) {
				var list = Entities.querySeries("GenericMetricService", $.extend({_metricName: metricName}, condition), "@site", "sum(value)", _intervalList[_intervalType][1]);
				var seriesList = nvd3.convert.eagle([list]);
				if(!$scope[seriesName]) $scope[seriesName] = seriesList;
				list._promise.then(function() {
					$scope[seriesName] = seriesList;
				});
			}

			function refreshSeries() {
				var _timeRange = _intervalList[_intervalType][2]();
				var _startTime = _timeRange[0];
				var _endTime = _timeRange[1];
				var _cond = {
					application: policy.tags.application,
					policyId: policy.tags.policyId,
					_startTime: _startTime,
					_endTime: _endTime
				};
				console.log("Range:", common.format.date(_startTime, "datetime"), common.format.date(_endTime, "datetime"));

				// > eagle.policy.eval.count
				_loadSeries("policyEvalSeries", "eagle.policy.eval.count", _cond);

				// > eagle.policy.eval.fail.count
				_loadSeries("policyEvalFailSeries", "eagle.policy.eval.fail.count", _cond);

				// > eagle.alert.count
				_loadSeries("alertSeries", "eagle.alert.count", _cond);

				// > eagle.alert.fail.count
				_loadSeries("alertFailSeries", "eagle.alert.fail.count", _cond);
			}

			// Alert list
			$scope.alertList = Entities.queryEntities("AlertService", {
				site: Site.current().tags.site,
				application: policy.tags.application,
				policyId: policy.tags.policyId,
				_pageSize: MAX_PAGESIZE,
				_duration: 1000 * 60 * 60 * 24 * 30,
				__ETD: 1000 * 60 * 60 * 24
			});

			refreshSeries();
			seriesRefreshInterval = $interval(refreshSeries, 60000);

			// Menu
			$scope.visualizationMenu = [
				{icon: "clock-o", title: "Interval", list:
					$.map(_intervalList, function(item, index) {
						var _item = {icon: "clock-o", title: item[0], func: function() {
							_intervalType = index;
							refreshSeries();
						}};
						Object.defineProperty(_item, 'strong', {
							get: function() {return _intervalType === index;}
						});
						return _item;
					})
				}
			];
		});

		// Function
		$scope.updatePolicyStatus = Policy.updatePolicyStatus;
		$scope.deletePolicy = function(policy) {
			Policy.deletePolicy(policy, function() {
				location.href = "#/common/policyList";
			});
		};

		// Clean up
		$scope.$on('$destroy', function() {
			$interval.cancel(seriesRefreshInterval);
		});
	});

	// ======================== Policy Edit =========================
	function policyCtrl(create, PageConfig, Site, Policy, $scope, $wrapState, $q, UI, Entities, Application, Authorization, Notification) {
		PageConfig.pageTitle = create ? "Policy Create" : "Policy Edit";
		PageConfig.pageSubTitle = Site.current().tags.site;
		PageConfig
			.addNavPath("Policy List", "/common/policyList")
			.addNavPath("Policy Edit");

		var _winTimeDesc = "Number unit[millisecond, sec, min, hour, day, month, year]. e.g. 23 sec";
		var _winTimeRegex = /^\d+\s+(millisecond|milliseconds|second|seconds|sec|minute|minutes|min|hour|hours|day|days|week|weeks|month|months|year|years)$/;
		var _winTimeDefaultValue = '10 min';
		$scope.config = {
			window: [
				// Display name, window type, required columns[Title, Description || "LONG_FIELD", Regex check, default value]
				{
					title: "Message Time Slide",
					description: "Using timestamp filed from input is used as event's timestamp",
					type: "externalTime",
					fields:[
						{title: "Field", defaultValue: "timestamp", hide: true},
						{title: "Time", description: _winTimeDesc, regex: _winTimeRegex, defaultValue: _winTimeDefaultValue}
					]
				},
				{
					title: "System Time Slide",
					description: "Using System time is used as timestamp for event's timestamp",
					type: "time",
					fields:[
						{title: "Time", description: _winTimeDesc, regex: _winTimeRegex, defaultValue: _winTimeDefaultValue}
					]
				},
				{
					title: "System Time Batch",
					description: "Same as System Time Window except the policy is evaluated at fixed duration",
					type: "timeBatch",
					fields:[
						{title: "Time", description: _winTimeDesc, regex: _winTimeRegex, defaultValue: _winTimeDefaultValue}
					]
				},
				{
					title: "Length Slide",
					description: "The slide window has a fixed length",
					type: "length",
					fields:[
						{title: "Number", description: "Number only. e.g. 1023", regex: /^\d+$/}
					]
				},
				{
					title: "Length Batch",
					description: "Same as Length window except the policy is evaluated in batch mode when fixed event count reached",
					type: "lengthBatch",
					fields:[
						{title: "Number", description: "Number only. e.g. 1023", regex: /^\d+$/}
					]
				}
			]
		};

		$scope.Notification = Notification;

		$scope.create = create;
		$scope.encodedRowkey = $wrapState.param.filter;

		$scope.step = 0;
		$scope.applications = {};
		$scope.policy = {};

		// ==========================================
		// =              Notification              =
		// ==========================================
		$scope.notificationTabHolder = null;

		$scope.newNotification = function (notificationType) {
			var __notification = {
				notificationType: notificationType
			};

			$.each(Notification.map[notificationType].fieldList, function (i, field) {
				__notification[field.name] = field.value || "";
			});

			$scope.policy.__.notification.push(__notification);
		};

		Notification.promise.then(function () {
			$scope.menu = Authorization.isRole('ROLE_ADMIN') ? [
				{icon: "cog", title: "Configuration", list: [
					{icon: "trash", title: "Delete", danger: true, func: function () {
						var notification = $scope.notificationTabHolder.selectedPane.data;
						UI.deleteConfirm(notification.notificationType).then(null, null, function(holder) {
							common.array.remove(notification, $scope.policy.__.notification);
							holder.closeFunc();
						});
					}}
				]},
				{icon: "plus", title: "New", list: $.map(Notification.list, function(notification) {
					return {
						icon: "plus",
						title: notification.tags.notificationType,
						func: function () {
							$scope.newNotification(notification.tags.notificationType);
							setTimeout(function() {
								$scope.notificationTabHolder.setSelect(-1);
								$scope.$apply();
							}, 0);
						}
					};
				})}
			] : [];
		});

		// ==========================================
		// =            Data Preparation            =
		// ==========================================
		// Steam list
		var _streamList = Entities.queryEntities("AlertStreamService", {application: Application.current().tags.application});
		var _executorList = Entities.queryEntities("AlertExecutorService", {application: Application.current().tags.application});
		$scope.streamList = _streamList;
		$scope.executorList = _executorList;
		$scope.streamReady = false;

		$q.all([_streamList._promise, _executorList._promise]).then(function() {
			// Map executor with stream
			$.each(_executorList, function(i, executor) {
				$.each(_streamList, function(j, stream) {
					if(stream.tags.application === executor.tags.application && stream.tags.streamName === executor.tags.streamName) {
						stream.alertExecutor = executor;
						return false;
					}
				});
			});

			// Fill stream list
			$.each(_streamList, function(i, unit) {
				var _srcStreamList = $scope.applications[unit.tags.application] = $scope.applications[unit.tags.application] || [];
				_srcStreamList.push(unit);
			});

			$scope.streamReady = true;

			// ==========================================
			// =                Function                =
			// ==========================================
			function _findStream(application, streamName) {
				var _streamList = $scope.applications[application];
				if(!_streamList) return null;

				for(var i = 0 ; i < _streamList.length ; i += 1) {
					if(_streamList[i].tags.streamName === streamName) {
						return _streamList[i];
					}
				}
				return null;
			}

			// ==========================================
			// =              Step control              =
			// ==========================================
			$scope.steps = [
				// >> Select stream
				{
					title: "Select Stream",
					ready: function() {
						return $scope.streamReady;
					},
					init: function() {
						$scope.policy.__.streamName = $scope.policy.__.streamName ||
							common.array.find($scope.policy.tags.application, _streamList, "tags.application").tags.streamName;
					},
					nextable: function() {
						var _streamName = common.getValueByPath($scope.policy, "__.streamName");
						if(!_streamName) return false;

						// Detect stream in current data source list
						return !!common.array.find(_streamName, $scope.applications[$scope.policy.tags.application], "tags.streamName");
					}
				},

				// >> Define Alert Policy
				{
					title: "Define Alert Policy",
					init: function() {
						// Normal mode will fetch meta list
						if(!$scope.policy.__.advanced) {
							var _stream = _findStream($scope.policy.tags.application, $scope.policy.__.streamName);
							$scope._stream = _stream;

							if(!_stream) {
								$.dialog({
									title: "OPS",
									content: "Stream not found! Current application don't match any stream."
								});
								return;
							}

							if(!_stream.metas) {
								_stream.metas = Entities.queryEntities("AlertStreamSchemaService", {application: $scope.policy.tags.application, streamName: $scope.policy.__.streamName});
								_stream.metas._promise.then(function() {
									_stream.metas.sort(function(a, b) {
										if(a.tags.attrName < b.tags.attrName) {
											return -1;
										} else if(a.tags.attrName > b.tags.attrName) {
											return 1;
										}
										return 0;
									});
								});
							}
						}
					},
					ready: function() {
						if(!$scope.policy.__.advanced) {
							return $scope._stream.metas._promise.$$state.status === 1;
						}
						return true;
					},
					nextable: function() {
						if($scope.policy.__.advanced) {
							// Check stream source
							$scope._stream = null;
							$.each($scope.applications[$scope.policy.tags.application], function(i, stream) {
								if(($scope.policy.__._expression || "").indexOf(stream.tags.streamName) !== -1) {
									$scope._stream = stream;
									return false;
								}
							});
							return $scope._stream;
						} else {
							// Window
							if($scope.policy.__.windowConfig) {
								var _winMatch = true;
								var _winConds = $scope.getWindow().fields;
								$.each(_winConds, function(i, cond) {
									if(!(cond.val || "").match(cond.regex)) {
										_winMatch = false;
										return false;
									}
								});
								if(!_winMatch) return false;

								// Aggregation
								if($scope.policy.__.groupAgg) {
									if(!$scope.policy.__.groupAggPath ||
										!$scope.policy.__.groupCondOp ||
										!$scope.policy.__.groupCondVal) {
										return false;
									}
								}
							}
						}
						return true;
					}
				},

				// >> Configuration & Notification
				{
					title: "Configuration & Notification",
					nextable: function() {
						return !!$scope.policy.tags.policyId;
					}
				}
			];

			// ==========================================
			// =              Policy Logic              =
			// ==========================================
			_streamList._promise.then(function() {
				// Initial policy entity
				if(create) {
					$scope.policy = {
						__: {
							toJSON: jQuery.noop,
							conditions: {},
							notification: [],
							dedupe: {
								alertDedupIntervalMin: 10
							},
							policy: {},
							window: "externalTime",
							group: "",
							groupAgg: "count",
							groupAggPath: "timestamp",
							groupCondOp: ">=",
							groupCondVal: "2"
						},
						description: "",
						enabled: true,
						prefix: "alertdef",
						remediationDef: "",
						tags: {
							application: Application.current().tags.application,
							policyType: "siddhiCEPEngine"
						}
					};

					// If configured data source
					if($wrapState.param.app) {
						$scope.policy.tags.application = $wrapState.param.app;
						if(common.array.find($wrapState.param.app, Site.current().applicationList, "tags.application")) {
							setTimeout(function() {
								$scope.changeStep(0, 2, false);
								$scope.$apply();
							}, 1);
						}
					}

					// Start step
					$scope.changeStep(0, 1, false);
					console.log($scope.policy);
				} else {
					var _policy = Entities.queryEntity("AlertDefinitionService", $scope.encodedRowkey);
					_policy._promise.then(function() {
						if(_policy.length) {
							$scope.policy = _policy[0];
							$scope.policy.__ = {
								toJSON: jQuery.noop
							};

							Site.current(Site.find($scope.policy.tags.site));
						} else {
							$.dialog({
								title: "OPS",
								content: "Policy not found!"
							}, function() {
								$wrapState.path("/common/policyList");
								$scope.$apply();
							});
							return;
						}

						var _application = Application.current();
						if(_application.tags.application !== $scope.policy.tags.application) {
							_application = Application.find($scope.policy.tags.application);
							if(_application) {
								Application.current(_application, false);
								console.log("Application not match. Do reload...");
								$wrapState.reload();
							} else {
								$.dialog({
									title: "OPS",
									content: "Application not found! Current policy don't match any application."
								}, function() {
									$location.path("/common/policyList");
									$scope.$apply();
								});
							}
							return;
						}

						// === Revert inner data ===
						// >> De-dupe
						$scope.policy.__.dedupe = common.parseJSON($scope.policy.dedupeDef, {});

						// >> Notification
						$scope.policy.__.notification = common.parseJSON($scope.policy.notificationDef, []);

						// >> Policy
						var _policyUnit = $scope.policy.__.policy = common.parseJSON($scope.policy.policyDef);

						// >> Parse expression
						$scope.policy.__.conditions = {};
						var _condition = _policyUnit.expression.match(/from\s+(\w+)(\[(.*)])?(#window[^\)]*\))?\s+(select (\w+, )?(\w+)\((\w+)\) as [\w\d_]+ (group by (\w+) )?having ([\w\d_]+) ([<>=]+) ([^\s]+))?/);
						var _cond_stream = _condition[1];
						var _cond_query = _condition[3] || "";
						var _cond_window = _condition[4];
						var _cond_group = _condition[5];
						var _cond_groupUnit = _condition.slice(7,14);

						if(!_condition) {
							$scope.policy.__.advanced = true;
						} else {
							// > StreamName
							var _streamName = _cond_stream;
							var _cond = _cond_query;

							$scope.policy.__.streamName = _streamName;

							// > Conditions
							// Loop condition groups
							if(_cond.trim() !== "" && /^\(.*\)$/.test(_cond)) {
								var _condGrps = _cond.substring(1, _cond.length - 1).split(/\)\s+and\s+\(/);
								$.each(_condGrps, function(i, line) {
									// Loop condition cells
									var _condCells = line.split(/\s+or\s+/);
									$.each(_condCells, function(i, cell) {
										var _opMatch = cell.match(/(\S*)\s*(==|!=|>|<|>=|<=|contains)\s*('(?:[^'\\]|\\.)*'|[\w\d]+)/);
										if(!common.isEmpty(_opMatch)) {
											var _key = _opMatch[1];
											var _op = _opMatch[2];
											var _val = _opMatch[3];
											var _conds = $scope.policy.__.conditions[_key] = $scope.policy.__.conditions[_key] || [];
											var _type = "";
											if(_val.match(/'.*'/)) {
												_val = _val.slice(1, -1);
												_type = "string";
											} else if(_val === "true" || _val === "false") {
												var _regexMatch = _key.match(/^str:regexp\((\w+),'(.*)'\)/);
												var _containsMatch = _key.match(/^str:contains\((\w+),'(.*)'\)/);
												var _mathes = _regexMatch || _containsMatch;
												if(_mathes) {
													_key = _mathes[1];
													_val = _mathes[2];
													_type = "string";
													_op = _regexMatch ? "regex" : "contains";
													_conds = $scope.policy.__.conditions[_key] = $scope.policy.__.conditions[_key] || [];
												} else {
													_type = "bool";
												}
											} else {
												_type = "number";
											}
											_conds.push($scope._CondUnit(_key, _op, _val, _type));
										}
									});
								});
							} else if(_cond_query !== "") {
								$scope.policy.__.advanced = true;
							}
						}

						if($scope.policy.__.advanced) {
							$scope.policy.__._expression = _policyUnit.expression;
						} else {
							// > window
							if(!_cond_window) {
								$scope.policy.__.window = "externalTime";
								$scope.policy.__.group = "";
								$scope.policy.__.groupAgg = "count";
								$scope.policy.__.groupAggPath = "timestamp";
								$scope.policy.__.groupCondOp = ">=";
								$scope.policy.__.groupCondVal = "2";
							} else {
								try {
									$scope.policy.__.windowConfig = true;

									var _winCells = _cond_window.match(/\.(\w+)\((.*)\)/);
									$scope.policy.__.window = _winCells[1];
									var _winConds = $scope.getWindow().fields;
									$.each(_winCells[2].split(","), function(i, val) {
										_winConds[i].val = val;
									});

									// Group
									if(_cond_group) {
										$scope.policy.__.group = _cond_groupUnit[3];
										$scope.policy.__.groupAgg = _cond_groupUnit[0];
										$scope.policy.__.groupAggPath = _cond_groupUnit[1];
										$scope.policy.__.groupAggAlias = _cond_groupUnit[4] === "aggValue" ? "" : _cond_groupUnit[4];
										$scope.policy.__.groupCondOp = _cond_groupUnit[5];
										$scope.policy.__.groupCondVal = _cond_groupUnit[6];
									} else {
										$scope.policy.__.group = "";
										$scope.policy.__.groupAgg = "count";
										$scope.policy.__.groupAggPath = "timestamp";
										$scope.policy.__.groupCondOp = ">=";
										$scope.policy.__.groupCondVal = "2";
									}
								} catch(err) {
									$scope.policy.__.window = "externalTime";
								}
							}
						}

						$scope.changeStep(0, 2, false);
						console.log($scope.policy);
					});
				}
			});

			// ==========================================
			// =                Function                =
			// ==========================================
			// UI: Highlight select step
			$scope.stepSelect = function(step) {
				return step === $scope.step ? "active" : "";
			};

			// UI: Collapse all
			$scope.collapse = function(cntr) {
				var _list = $(cntr).find(".collapse").css("height", "auto");
				if(_list.hasClass("in")) {
					_list.removeClass("in");
				} else {
					_list.addClass("in");
				}
			};

			// Step process. Will fetch target step attribute and return boolean
			function _check(key, step) {
				var _step = $scope.steps[step - 1];
				if(!_step) return;

				var _value = _step[key];
				if(typeof _value === "function") {
					return _value();
				} else if(typeof _value === "boolean") {
					return _value;
				}
				return true;
			}
			// Check step is ready. Otherwise will display load animation
			$scope.stepReady = function(step) {
				return _check("ready", step);
			};
			// Check whether process next step. Otherwise will disable next button
			$scope.checkNextable = function(step) {
				return !_check("nextable", step);
			};
			// Switch step
			$scope.changeStep = function(step, targetStep, check) {
				if(check === false || _check("checkStep", step)) {
					$scope.step =  targetStep;

					_check("init", targetStep);
				}
			};

			// Window
			$scope.getWindow = function() {
				if(!$scope.policy || !$scope.policy.__) return null;
				return common.array.find($scope.policy.__.window, $scope.config.window, "type");
			};

			// Aggregation
			$scope.groupAggPathList = function() {
				return $.grep(common.getValueByPath($scope, "_stream.metas", []), function(meta) {
					return $.inArray(meta.attrType, ['long','integer','number', 'double', 'float']) !== -1;
				});
			};

			$scope.updateGroupAgg = function() {
				$scope.policy.__.groupAggPath = $scope.policy.__.groupAggPath || common.getValueByPath($scope.groupAggPathList()[0], "tags.attrName");

				if($scope.policy.__.groupAgg === 'count') {
					$scope.policy.__.groupAggPath = 'timestamp';
				}
			};

			// Resolver
			$scope.resolverTypeahead = function(value, resolver) {
				var _resolverList = Entities.query("stream/attributeresolve", {
					site: Site.current().tags.site,
					resolver: resolver,
					query: value
				});
				return _resolverList._promise.then(function() {
					return _resolverList;
				});
			};

			// Used for input box when pressing enter
			$scope.conditionPress = function(event) {
				if(event.which == 13) {
					setTimeout(function() {
						$(event.currentTarget).closest(".input-group").find("button").click();
					}, 1);
				}
			};
			// Check whether has condition
			$scope.hasCondition = function(key, type) {
				var _list = common.getValueByPath($scope.policy.__.conditions, key, []);
				if(_list.length === 0) return false;

				if(type === "bool") {
					return !_list[0].ignored();
				}
				return true;
			};
			// Condition unit definition
			$scope._CondUnit = function(key, op, value, type) {
				return {
					key: key,
					op: op,
					val: value,
					type: type,
					ignored: function() {
						return this.type === "bool" && this.val === "none";
					},
					getVal: function() {
						return this.type === "string" ? "'" + this.val + "'" : this.val;
					},
					toString: function() {
						return this.op + " " + this.getVal();
					},
					toCondString: function() {
						var _op = this.op === "=" ? "==" : this.op;
						if(_op === "regex") {
							return "str:regexp(" + this.key + "," + this.getVal() + ")==true";
						} else if(_op === "contains") {
							return "str:contains(" + this.key + "," + this.getVal() + ")==true";
						} else {
							return this.key + " " + _op + " " + this.getVal();
						}
					}
				};
			};
			// Add condition for policy
			$scope.addCondition = function(key, op, value, type) {
				if(value === "" || value === undefined) return false;

				var _condList = $scope.policy.__.conditions[key] = $scope.policy.__.conditions[key] || [];
				_condList.push($scope._CondUnit(key, op, value, type));
				return true;
			};
			// Convert condition list to description string
			$scope.parseConditionDesc = function(key) {
				return $.map($scope.policy.__.conditions[key] || [], function(cond) {
					if(!cond.ignored()) return "[" + cond.toString() + "]";
				}).join(" or ");
			};

			// To query
			$scope.toQuery = function() {
				if(!$scope.policy.__) return "";

				if($scope.policy.__.advanced) return $scope.policy.__._expression;

				// > Query
				var _query = $.map(common.getValueByPath($scope.policy, "__.conditions", {}), function(list) {
					var _conds = $.map(list, function(cond) {
						if(!cond.ignored()) return cond.toCondString();
					});
					if(_conds.length) {
						return "(" + _conds.join(" or ") + ")";
					}
				}).join(" and ");
				if(_query) {
					_query = "[" + _query + "]";
				}

				// > Window
				var _window = $scope.getWindow();
				var _windowStr = "";
				if($scope.policy.__.windowConfig) {
					_windowStr = $.map(_window.fields, function(field) {
						return field.val;
					}).join(",");
					_windowStr = "#window." + _window.type + "(" + _windowStr + ")";

					// > Group
					if($scope.policy.__.group) {
						_windowStr += common.template(" select ${group}, ${groupAgg}(${groupAggPath}) as ${groupAggAlias} group by ${group} having ${groupAggAlias} ${groupCondOp} ${groupCondVal}", {
							group: $scope.policy.__.group,
							groupAgg: $scope.policy.__.groupAgg,
							groupAggPath: $scope.policy.__.groupAggPath,
							groupCondOp: $scope.policy.__.groupCondOp,
							groupCondVal: $scope.policy.__.groupCondVal,
							groupAggAlias: $scope.policy.__.groupAggAlias || "aggValue"
						});
					} else {
						_windowStr += common.template(" select ${groupAgg}(${groupAggPath}) as ${groupAggAlias} having ${groupAggAlias} ${groupCondOp} ${groupCondVal}", {
							groupAgg: $scope.policy.__.groupAgg,
							groupAggPath: $scope.policy.__.groupAggPath,
							groupCondOp: $scope.policy.__.groupCondOp,
							groupCondVal: $scope.policy.__.groupCondVal,
							groupAggAlias: $scope.policy.__.groupAggAlias || "aggValue"
						});
					}
				} else {
					_windowStr = " select *";
				}

				return common.template("from ${stream}${query}${window} insert into outputStream;", {
					stream: $scope.policy.__.streamName,
					query: _query,
					window: _windowStr
				});
			};

			// ==========================================
			// =             Update Policy              =
			// ==========================================
			// dedupeDef notificationDef policyDef
			$scope.finishPolicy = function() {
				$scope.lock = true;

				// dedupeDef
				$scope.policy.dedupeDef = JSON.stringify($scope.policy.__.dedupe);

				// notificationDef
				$scope.policy.__.notification = $scope.policy.__.notification || [];

				$scope.policy.notificationDef = JSON.stringify($scope.policy.__.notification);

				// policyDef
				$scope.policy.__._dedupTags = $scope.policy.__._dedupTags || {};
				$scope.policy.__.policy = {
					expression: $scope.toQuery(),
					type: "siddhiCEPEngine"
				};
				$scope.policy.policyDef = JSON.stringify($scope.policy.__.policy);

				// alertExecutorId
				if($scope._stream.alertExecutor) {
					$scope.policy.tags.alertExecutorId = $scope._stream.alertExecutor.tags.alertExecutorId;
				} else {
					$scope.lock = false;
					$.dialog({
						title: "OPS!",
						content: "Alert Executor not defined! Please check 'AlertExecutorService'!"
					});
					return;
				}

				// site
				$scope.policy.tags.site = $scope.policy.tags.site || Site.current().tags.site;

				// owner
				$scope.policy.owner = Authorization.userProfile.username;

				// Update function
				function _updatePolicy() {
					Entities.updateEntity("AlertDefinitionService", $scope.policy)._promise.success(function(data) {
						$.dialog({
							title: "Success",
							content: (create ? "Create" : "Update") + " success!"
						}, function() {
							if(data.success) {
								location.href = "#/common/policyList";
							} else {
								$.dialog({
									title: "OPS",
									content: (create ? "Create" : "Update") + "failed!" + JSON.stringify(data)
								});
							}
						});

						$scope.create = create = false;
						$scope.encodedRowkey = data.obj[0];
					}).error(function(data) {
						$.dialog({
							title: "OPS",
							content: (create ? "Create" : "Update") + "failed!" + JSON.stringify(data)
						});
					}).then(function() {
						$scope.lock = false;
					});
				}

				// Check if already exist
				if($scope.create) {
					var _checkList = Entities.queryEntities("AlertDefinitionService", {
						alertExecutorId: $scope.policy.tags.alertExecutorId,
						policyId: $scope.policy.tags.policyId,
						policyType: "siddhiCEPEngine",
						application: $scope.policy.tags.application
					});
					_checkList._promise.then(function() {
						if(_checkList.length) {
							$.dialog({
								title: "Override Confirm",
								content: "Already exists PolicyID '" + $scope.policy.tags.policyId + "'. Do you want to override?",
								confirm: true
							}, function(ret) {
								if(ret) {
									_updatePolicy();
								} else {
									$scope.lock = false;
									$scope.$apply();
								}
							});
						} else {
							_updatePolicy();
						}
					});
				} else {
					_updatePolicy();
				}
			};
		});
	}

	feature.controller('policyCreate', function(PageConfig, Site, Policy, $scope, $wrapState, $q, UI, Entities, Application, Authorization, Notification) {
		var _args = [true];
		_args.push.apply(_args, arguments);
		policyCtrl.apply(this, _args);
	}, "policyEdit");
	feature.controller('policyEdit', function(PageConfig, Site, Policy, $scope, $wrapState, $q, UI, Entities, Application, Authorization, Notification) {
		PageConfig.lockSite = true;
		var _args = [false];
		_args.push.apply(_args, arguments);
		policyCtrl.apply(this, _args);
	});

	// ==============================================================
	// =                           Alerts                           =
	// ==============================================================

	// ========================= Alert List =========================
	feature.navItem("alertList", "Alerts", "exclamation-triangle");
	feature.controller('alertList', function(PageConfig, Site, $scope, $wrapState, $interval, $timeout, Entities, Application) {
		PageConfig.pageSubTitle = Site.current().tags.site;

		var MAX_PAGESIZE = 10000;

		// Initial load
		$scope.application = Application.current();

		$scope.alertList = [];
		$scope.alertList.ready = false;

		// Load data
		function _loadAlerts() {
			if($scope.alertList._promise) {
				$scope.alertList._promise.abort();
			}

			var _list = Entities.queryEntities("AlertService", {
				site: Site.current().tags.site,
				application: $scope.application.tags.application,
				_pageSize: MAX_PAGESIZE,
				_duration: 1000 * 60 * 60 * 24 * 30,
				__ETD: 1000 * 60 * 60 * 24
			});
			$scope.alertList._promise = _list._promise;
			_list._promise.then(function() {
				var index;

				if($scope.alertList[0]) {
					// List new alerts
					for(index = 0 ; index < _list.length ; index += 1) {
						var _alert = _list[index];
						_alert.__new = true;
						if(_alert.encodedRowkey === $scope.alertList[0].encodedRowkey) {
							break;
						}
					}

					if(index > 0) {
						$scope.alertList.unshift.apply($scope.alertList, _list.slice(0, index));

						// Clean up UI highlight
						$timeout(function() {
							$.each(_list, function(i, alert) {
								delete alert.__new;
							});
						}, 100);
					}
				} else {
					// List all alerts
					$scope.alertList.push.apply($scope.alertList, _list);
				}

				$scope.alertList.ready = true;
			});
		}

		_loadAlerts();
		var _loadInterval = $interval(_loadAlerts, app.time.refreshInterval);
		$scope.$on('$destroy',function(){
			$interval.cancel(_loadInterval);
		});
	});

	// ======================== Alert Detail ========================
	feature.controller('alertDetail', function(PageConfig, Site, $scope, $wrapState, Entities) {
		PageConfig.pageTitle = "Alert Detail";
		PageConfig.lockSite = true;
		PageConfig
			.addNavPath("Alert List", "/common/alertList")
			.addNavPath("Alert Detail");

		$scope.common = common;

		// Query policy
		$scope.alertList = Entities.queryEntity("AlertService", $wrapState.param.filter);
		$scope.alertList._promise.then(function() {
			if($scope.alertList.length === 0) {
				$.dialog({
					title: "OPS!",
					content: "Alert not found!"
				}, function() {
					location.href = "#/common/alertList";
				});
			} else {
				$scope.alert = $scope.alertList[0];
				$scope.alert.rawAlertContext = JSON.stringify($scope.alert.alertContext, null, "\t");
				Site.current(Site.find($scope.alert.tags.site));
				console.log($scope.alert);
			}
		});

		// UI
		$scope.getMessageTime = function(alert) {
			var _time = common.getValueByPath(alert, "alertContext.timestamp");
			return Number(_time);
		};
	});
})();