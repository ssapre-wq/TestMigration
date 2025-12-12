import datetime

from TM1py.Exceptions import TM1pyException
from TM1py.Services import TM1Service
from TM1py.Utils import Utils

from PyPromote.Services import DimensionService


class CubeService:
    """

    """

    def __init__(self, source: TM1Service, target: TM1Service, server: TM1Service):
        self.source = source
        self.target = target
        self.server = server
        self.dimensions = DimensionService(source=self.source, target=self.target, server=self.server)
        view="Default" 

    def copy_cube(self, cube: str, item: str, deployment: str):
        start_time = datetime.datetime.now()
        try:
            if self.source.cubes.exists(cube_name=cube):
                cub = self.source.cubes.get(cube_name=cube)
                if self.target.cubes.exists(cube_name=cube):
                    self.target.cubes.update(cube=cub)
                    message = f"Cube: '{cube}' updated"
                else:
                    for dim in cub.dimensions:
                        if not self.target.dimensions.exists(dimension_name=dim):
                            _dim = self.source.dimensions.get(dimension_name=dim)
                            self.target.dimensions.update_or_create(dimension=_dim)
                    self.target.cubes.create(cube=cub)
                    message = f"Cube '{cube}' created"
                end_time = datetime.datetime.now()
                duration = end_time - start_time
                cellset = dict()
                cellset[(deployment, item, "Deployment Status")] = message
                cellset[(deployment, item, "Deployment Start")] = datetime.datetime.strftime(start_time,
                                                                                             '%Y-%m-%d %H:%M:%S')
                cellset[(deployment, item, "Deployment End")] = datetime.datetime.strftime(end_time,
                                                                                           '%Y-%m-%d %H:%M:%S')
                cellset[(deployment, item, "Deployment Duration")] = str(duration)
                self.server.cubes.cells.write_values('System - Deployments', cellset)

		#Get data                
		viewcelldata = server.cubes.cells.execute_view(cube_name=cube,view_name=view)

		#Write data
		server.cubes.cells.write_values(cube, viewcelldata)
                
     
            else:
                return "Source cube does not exist"
        except TM1pyException as t:
            print(t)

    def copy_rule(self, cube: str, item: str, deployment: str):
        start_time = datetime.datetime.now()
        try:
            if self.source.cubes.exists(cube_name=cube):
                if self.target.cubes.exists(cube_name=cube):
                    source_cube = self.source.cubes.get(cube_name=cube)
                    target_cube = self.target.cubes.get(cube_name=cube)
                    if source_cube.has_rules:
                        target_cube.rules = source_cube.rules
                        message = f"Rule updated on Target Cube: '{cube}'"
                    else:
                        message = f"Source cube: '{cube}' does not have rules"
                else:
                    message = f"Target cube '{cube}' does not exist migrate cube."
            else:
                message = f"Source cube: '{cube}' does not exist."
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            cellset = dict()
            cellset[(deployment, item, "Deployment Status")] = message
            cellset[(deployment, item, "Deployment Start")] = datetime.datetime.strftime(start_time,
                                                                                         '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment End")] = datetime.datetime.strftime(end_time,
                                                                                       '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment Duration")] = str(duration)
            self.server.cubes.cells.write_values('System - Deployments', cellset)
        except TM1pyException as t:
            print(t)

    def copy_view(self, cube: str, view: str, item: str, deployment: str):
        start_time = datetime.datetime.now()
        try:
            if self.source.views.exists(cube_name=cube, view_name=view, private=False):
                source_view = self.source.views.get(cube_name=cube, view_name=view, private=False)
                dimensions = []
                dimensions.extend(list(source_view.titles))
                dimensions.extend(source_view.rows)
                dimensions.extend(source_view.columns)
                for dimension in dimensions:
                    sub = dimension.subset.name
                    if sub:
                        self.dimensions.copy_subset(dimension=dimension.dimension_name,
                                                    subset=sub,
                                                    item=item,
                                                    deployment=deployment)
                if self.target.views.exists(cube_name=cube, view_name=view, private=False):
                    self.target.views.update(view=source_view, private=False)
                    message = f"Target view: '{view}' updated"
                else:
                    self.target.views.create(view=source_view, private=False)
                    message = f"Source view: '{view}' created"
            else:
                message = f"Source view '{view}' does not exist."
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            cellset = dict()
            cellset[(deployment, item, "Deployment Status")] = message
            cellset[(deployment, item, "Deployment Start")] = datetime.datetime.strftime(start_time,
                                                                                         '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment End")] = datetime.datetime.strftime(end_time,
                                                                                       '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment Duration")] = str(duration)
            self.server.cubes.cells.write_values('System - Deployments', cellset)
        except TM1pyException as t:
            print(t)

    def copy_attributes(self, dimension: str, item: str, deployment: str):
        start_time = datetime.datetime.now()
        try:
            if self.source.dimensions.exists(dimension_name=dimension):
                attr_cube = "}ElementAttributes_" + dimension
                if self.source.cubes.exists(cube_name=attr_cube):
                    mdx = "SELECT " \
                          "NON EMPTY {TM1SUBSETALL( [" + dimension + "])} ON ROWS, " \
                          "NON EMPTY {TM1SUBSETALL( [" + attr_cube + "])} ON COLUMNS " \
                          "FROM [" + attr_cube + "]"
                    data = self.source.cells.execute_mdx(mdx, ['Value'])
                    df = Utils.build_pandas_dataframe_from_cellset(data)
                    cellset = Utils.build_cellset_from_pandas_dataframe(df)
                    if not self.target.cubes.exists(cube_name=attr_cube):
                        self.copy_cube(cube=attr_cube, item=item, deployment=deployment)
                    self.target.cells.write(cube_name=attr_cube, cellset_as_dict=cellset)
                    message = 'Source Attributes Copied'
                else:
                    message = f"Source dimension: {dimension} does not have attributes"
            else:
                message = "Source dimension does not exist"
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            cellset = dict()
            cellset[(deployment, item, "Deployment Status")] = message
            cellset[(deployment, item, "Deployment Start")] = datetime.datetime.strftime(start_time,
                                                                                         '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment End")] = datetime.datetime.strftime(end_time,
                                                                                       '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment Duration")] = str(duration)
            self.server.cubes.cells.write_values('System - Deployments', cellset)
        except TM1pyException as t:
            print(t)