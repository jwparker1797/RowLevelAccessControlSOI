// Copyright 2018 ESRI
// 
// All rights reserved under the copyright laws of the United States
// and applicable international laws, treaties, and conventions.
// 
// You may freely redistribute and use this sample code, with or
// without modification, provided you include the original copyright
// notice and use restrictions.
// 
// See the use restrictions at <your Enterprise SDK install location>/userestrictions.txt.
// 

using ESRI.ArcGIS.Carto;
using ESRI.ArcGIS.esriSystem;
using ESRI.ArcGIS.Geodatabase;
using ESRI.ArcGIS.Geometry;
using ESRI.ArcGIS.Server;
using ESRI.Server.SOESupport;
using ESRI.Server.SOESupport.SOI;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Web.Script.Serialization;

//This is SOI template of Enterprise SDK

namespace RowLevelAccessControlSOI
{
    [ComVisible(true)]
    [Guid("6f1424fa-d840-4ac7-b36a-6dc11ca859e2")]
    [ClassInterface(ClassInterfaceType.None)]
    [ServerObjectInterceptor("MapServer",
         Description = "Filters records based upon groups of which a user is a member. Currently only implemented on REST services.",
        DisplayName = "Row Level Access Control SOI",
        Properties = "GroupNameAttributeField=;GroupNameForAllData=",
        SupportsSharedInstances = true)]
    public class RowLevelAccessControlSOI : IServerObjectExtension, IRESTRequestHandler, IWebRequestHandler, IRequestHandler2, IRequestHandler, IObjectConstruct
    {
        private string _soiName;
        private IServerObjectHelper _soHelper;
        private ServerLogger _serverLog;
        private RestSOIHelper _restSOIHelper;
        private static string groupNameFieldAttr;
        private static string groupNameForAllData;

        public RowLevelAccessControlSOI()
        {
            _soiName = this.GetType().Name;
        }

        public void Init(IServerObjectHelper pSOH)
        {
            _soHelper = pSOH;
            _serverLog = new ServerLogger();
            _restSOIHelper = new RestSOIHelper(pSOH);
            _serverLog.LogMessage(ServerLogger.msgType.infoStandard, _soiName + ".init()", 200, "Initialized " + _soiName + " SOI.");
        }

        public void Shutdown()
        {
            _serverLog.LogMessage(ServerLogger.msgType.infoStandard, _soiName + ".init()", 200, "Shutting down " + _soiName + " SOI.");
        }

        public void Construct(IPropertySet props)
        {
            _serverLog.LogMessage(ServerLogger.msgType.infoStandard, _soiName + ".Construct()", 200, "GroupNameAttributeField: " + props.GetProperty("GroupNameAttributeField").ToString());
            _serverLog.LogMessage(ServerLogger.msgType.infoStandard, _soiName + ".Construct()", 200, "GroupNameForAllData: " + props.GetProperty("GroupNameForAllData").ToString());
            groupNameFieldAttr = props.GetProperty("GroupNameAttributeField").ToString();
            groupNameForAllData = props.GetProperty("GroupNameForAllData").ToString();
        }

        #region Access Filters
        private string CreateGroupWhereClause()
        {
            var userRoleSet = ServerUtilities.GetGroupInfo(ServerUtilities.GetServerEnvironment());
            if (userRoleSet == null)
            {
                _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".CreateGroupWhereClause()", 200, "No groups found for user.");
                return "1=0";
            }
            _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".CreateGroupWhereClause()", 200, "User groups: " + String.Join(", ", userRoleSet));
            if (userRoleSet.Contains(groupNameForAllData))
            {
                _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".CreateGroupWhereClause()", 200, "User a member of all inclusive group.");
                return "1=1";
            }
            else if (userRoleSet.Any())
            {
                _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".CreateGroupWhereClause()", 200, "User is a member of a (any) group.");
                string outWhere = "";
                foreach (var group in userRoleSet)
                {
                    if (group == userRoleSet.First())
                        outWhere += "(" + groupNameFieldAttr + "='" + group + "'";
                    else
                        outWhere += " OR " + groupNameFieldAttr + "='" + group + "'";
                }
                outWhere += ")";
                return outWhere;
            }
            else
            {
                _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".CreateGroupWhereClause()", 200, "User is not a member of a (any) group.");
                return "1=0";
            }
        }
        private bool isValidSQLClause(IRESTRequestHandler restRequestHandler, string Capabilities, string resourceName, string groupWhereClause)
        {
            byte[] validateSQLResponse = restRequestHandler.HandleRESTRequest(Capabilities, resourceName, "validateSQL", "{\"sqlType\":\"where\",\"sql\":\"" + groupWhereClause + "\"}", "json", "{\"computeETag\":true,\"ETag\":\"57bf5a93\"}", out string sqlResponseProperties);
            string validateSQLResult = Encoding.UTF8.GetString(validateSQLResponse);
            JavaScriptSerializer sr = new JavaScriptSerializer { MaxJsonLength = int.MaxValue };
            var resultJSON = sr.DeserializeObject(validateSQLResult) as IDictionary<string, object>;
            if (resultJSON.Keys.Contains("isValidSQL"))
                return (bool)resultJSON["isValidSQL"];
            else
                return true;
        }
        private string[] GetShownLayerResourceNamesFromMapServiceRequest(string layers)
        {
            string[] resourceNames = layers.Split(':')[1].Split(',');
            return resourceNames;
        }
        #endregion

        #region REST interceptors

        public string GetSchema()
        {
            IRESTRequestHandler restRequestHandler = _restSOIHelper.FindRequestHandlerDelegate<IRESTRequestHandler>();
            if (restRequestHandler == null)
                return null;

            return restRequestHandler.GetSchema();
        }

        public byte[] HandleRESTRequest(string Capabilities, string resourceName, string operationName,
            string operationInput, string outputFormat, string requestProperties, out string responseProperties)
        {
            responseProperties = null;
            _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".HandleRESTRequest()", 200, "Request start. Resource: " + resourceName);
            try
            {
                IRESTRequestHandler restRequestHandler = _restSOIHelper.FindRequestHandlerDelegate<IRESTRequestHandler>();
                if (operationName == "query")
                {
                    string groupWhereClause = CreateGroupWhereClause();
                    _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".HandleRESTRequest()", 200, "Group Where Clause: " + groupWhereClause);
                    bool isValidSQL = isValidSQLClause(restRequestHandler, Capabilities, resourceName, groupWhereClause);
                    _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".HandleRESTRequest()", 200, "Is valid SQL?: " + isValidSQL.ToString());
                    if (isValidSQL)
                    {
                        JsonObject joOperationInput = new JsonObject(operationInput);
                        bool currentWhereFound = joOperationInput.TryGetString("where", out string currentWhere);
                        string whereClause;
                        if (currentWhere == "" || currentWhere == null)
                            whereClause = groupWhereClause;
                        else
                            whereClause = currentWhere + " AND " + groupWhereClause;
                        _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".HandleRESTRequest()", 200, "Final full where clause: " + whereClause);
                        joOperationInput.Delete("where");
                        joOperationInput.AddString("where", whereClause);
                        operationInput = joOperationInput.ToJson();
                    }
                }else if (operationName == "export")
                {
                    string groupWhereClause = CreateGroupWhereClause();
                    _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".HandleRESTRequest()", 200, "Group Where Clause: " + groupWhereClause);

                    JsonObject joOperationInput = new JsonObject(operationInput);
                    bool layersAttrFound = joOperationInput.TryGetString("layers", out string layers);
                    if (layersAttrFound)
                    {
                        string[] showLayers = GetShownLayerResourceNamesFromMapServiceRequest(layers);
                        _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".HandleRESTRequest()", 200, "Map Export - Shown Layers: " + String.Join(",", showLayers));

                        bool layerDefsAttrFound = joOperationInput.TryGetJsonObject("layerDefs", out JsonObject joLayerDefs);
                        if (!layerDefsAttrFound)
                            joLayerDefs = new JsonObject();
                        foreach (string layer in showLayers)
                        {
                            string finalLayerDef = groupWhereClause;
                            bool layerFound = joOperationInput.TryGetString(layer, out string existingLayerDef);
                            if (layerFound)
                            {
                                finalLayerDef += " AND " + existingLayerDef;
                            }
                            joLayerDefs.Delete(layer);
                            joLayerDefs.AddString(layer, finalLayerDef);
                        }
                        _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".HandleRESTRequest()", 200, "New Layer Def's: " + joLayerDefs.ToString());
                        joOperationInput.Delete("layerDefs");
                        joOperationInput.AddJsonObject("layerDefs", joLayerDefs);
                        operationInput = joOperationInput.ToJson();
                    }


                }else if (operationName == "find")
                {
                    return Encoding.UTF8.GetBytes("{\"error\":{\"code\":404,\"message\":\"Unable to complete operation.\",\"details\":This method is not allowed.\"]}}");
                }
                _serverLog.LogMessage(ServerLogger.msgType.debug, _soiName + ".HandleRESTRequest()", 200, "Sending Request");
                return restRequestHandler.HandleRESTRequest(
                        Capabilities, resourceName, operationName, operationInput,
                        outputFormat, requestProperties, out responseProperties);
            }
            catch (RestErrorException restError)
            {
                responseProperties = "{\"Content-Type\":\"text/plain;charset=utf-8\"}";
                return Encoding.UTF8.GetBytes(restError.Message);
            }
        }

        #endregion

        #region SOAP interceptors

        public byte[] HandleStringWebRequest(esriHttpMethod httpMethod, string requestURL,
            string queryString, string Capabilities, string requestData,
            out string responseContentType, out esriWebResponseDataType respDataType)
        {
            _serverLog.LogMessage(ServerLogger.msgType.infoStandard, _soiName + ".HandleStringWebRequest()",
                200, "Request received in Sample Object Interceptor for HandleStringWebRequest");

            /*
             * Add code to manipulate requests here
             */

            IWebRequestHandler webRequestHandler = _restSOIHelper.FindRequestHandlerDelegate<IWebRequestHandler>();
            if (webRequestHandler != null)
            {
                return webRequestHandler.HandleStringWebRequest(
                        httpMethod, requestURL, queryString, Capabilities, requestData, out responseContentType, out respDataType);
            }

            responseContentType = null;
            respDataType = esriWebResponseDataType.esriWRDTPayload;
            //Insert error response here.
            return null;
        }

        public byte[] HandleBinaryRequest(ref byte[] request)
        {
            _serverLog.LogMessage(ServerLogger.msgType.infoStandard, _soiName + ".HandleBinaryRequest()",
                  200, "Request received in Sample Object Interceptor for HandleBinaryRequest");

            /*
             * Add code to manipulate requests here
             */

            IRequestHandler requestHandler = _restSOIHelper.FindRequestHandlerDelegate<IRequestHandler>();
            if (requestHandler != null)
            {
                return requestHandler.HandleBinaryRequest(request);
            }

            //Insert error response here.
            return null;
        }

        public byte[] HandleBinaryRequest2(string Capabilities, ref byte[] request)
        {
            _serverLog.LogMessage(ServerLogger.msgType.infoStandard, _soiName + ".HandleBinaryRequest2()",
                  200, "Request received in Sample Object Interceptor for HandleBinaryRequest2");

            /*
             * Add code to manipulate requests here
             */

            IRequestHandler2 requestHandler = _restSOIHelper.FindRequestHandlerDelegate<IRequestHandler2>();
            if (requestHandler != null)
            {
                return requestHandler.HandleBinaryRequest2(Capabilities, request);
            }

            //Insert error response here.
            return null;
        }

        public string HandleStringRequest(string Capabilities, string request)
        {
            _serverLog.LogMessage(ServerLogger.msgType.infoStandard, _soiName + ".HandleStringRequest()",
                   200, "Request received in Sample Object Interceptor for HandleStringRequest");

            /*
             * Add code to manipulate requests here
             */

            IRequestHandler requestHandler = _restSOIHelper.FindRequestHandlerDelegate<IRequestHandler>();
            if (requestHandler != null)
            {
                return requestHandler.HandleStringRequest(Capabilities, request);
            }

            //Insert error response here.
            return null;
        }

        #endregion

    }
}
