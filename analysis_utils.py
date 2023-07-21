import pandas as pd
import seaborn as sns
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


def add_complexity_to_metadata(metadata):
    """
    Add a complexity score to metadata based on normalized calls, assignments, attributes, and size.
    
    Args:
        metadata (pd.DataFrame): The metadata DataFrame containing 'calls', 'assignments', 'attributes', and 'size' columns.

    Returns:
        pd.DataFrame: The modified metadata DataFrame with an additional 'complexity' column.
    """
    metadata_normalized = metadata[['calls', 'assignments', 'attributes', 'size']].apply(lambda x: (x - x.min()) / (x.quantile(0.99) - x.min()), axis=0)
    metadata['complexity'] = metadata_normalized.sum(axis=1)
    metadata_normalized.loc[(metadata_normalized['calls'] == 0) & (metadata_normalized['assignments'] == 0) & (metadata_normalized['attributes'] == 0), 'complexity'] = 0

    return metadata


def group_small_percentages(df, threshold):
    """
    Group small percentages in the DataFrame by combining libraries and components below the given threshold.
    
    Args:
        df (pd.DataFrame): The DataFrame containing library usage information.
        threshold (float): The threshold for grouping small percentages.

    Returns:
        pd.DataFrame: The modified DataFrame with grouped libraries and components.
    """
    # Group all libraries that contribute to less than threshold percent of all
    sum_of_all = df.groupby('library_name')['library_usage_count'].max().reset_index()['library_usage_count'].sum()
    df.loc[(df['library_usage_count'] / sum_of_all) < threshold, 'component'] = '<other>'
    df = df.groupby(['library_name', 'component']).agg({'count': 'sum', 'library_usage_count': 'max'}).reset_index()
    df.loc[df['component'] == '<other>', 'library_name'] = '<other>'
    df = df.groupby(['library_name', 'component']).agg({'count': 'sum', 'library_usage_count': 'sum'}).reset_index()

    # Group all components of library that contribute to less than threshold percent of all
    df.loc[(df['count'] / sum_of_all) < threshold, 'component'] = '<other>'
    df = df.groupby(['library_name', 'component']).agg({'count': 'sum', 'library_usage_count': 'max'}).reset_index()

    return df


def prepare_std_lib_treemap_data(libraries, component_types=None, threshold=0.002):
    """
    Prepare standard library treemap data by filtering component types and grouping small percentages.
    
    Args:
        libraries (pd.DataFrame): The DataFrame containing library usage information.
        component_types (list, optional): The list of component types to filter. Default is None.
        threshold (float, optional): The threshold for grouping small percentages. Default is 0.002.

    Returns:
        pd.DataFrame: The prepared DataFrame containing the necessary information for creating the treemap.
    """
    if component_types is not None:
        libraries = libraries[libraries['component_type'].isin(component_types)]

    library_usage = libraries.groupby('library_name').agg('sum', numeric_only=True).reset_index().rename(columns={'count': 'library_usage_count'})
    component_usage = libraries.groupby(['library_name', 'component']).agg('sum', numeric_only=True).reset_index()

    component_usage = component_usage.merge(library_usage, on='library_name')
    component_usage = group_small_percentages(component_usage, threshold)
    component_usage['library_percentage'] = component_usage['count'] / component_usage['library_usage_count'] * 100
    total_usage_count = component_usage['count'].sum()
    library_usage['total_percentage'] = library_usage['library_usage_count'] / total_usage_count * 100
    component_usage['total_percentage'] = component_usage['count'] / total_usage_count * 100

    return component_usage


# def create_std_lib_treemap(component_usage, title):
#     """
#     Create a treemap visualization of standard library usage.
    
#     Args:
#         component_usage (pd.DataFrame): The DataFrame containing the prepared data for the treemap.
#         title (str): The title of the treemap.

#     Returns:
#         None: The function displays the treemap using Plotly.
#     """
#     hovertemplate = "%{label}<br>Count: %{customdata[0]}<br>Share of library: %{customdata[1]:.2f}%<br>Share of all: %{customdata[2]:.2f}%"

#     fig = px.treemap(
#         component_usage,
#         path=['library_name', 'component'],
#         values='count',
#         color='library_percentage',
#         custom_data=['count', 'library_percentage', 'total_percentage'],
#         title=title,
#         color_continuous_scale='RdBu',
#         labels={'library_name': 'Library',
#                 'component': 'Component',
#                 'count': 'Count'}
#     )
    
#     fig.update_traces(hovertemplate=hovertemplate, textinfo='label+value+percent parent')

#     fig.update_layout(
#         width=1200,
#         height=1600,
#         title={
#             'text': title,
#             'font': {'size': 30},
#             'x': 0.5,
#             'y': 0.95,
#             'xanchor': 'center',
#             'yanchor': 'top'
#         },
#         margin=dict(l=10, r=10, t=100, b=10)
#     )
#     fig.show()


def create_std_lib_treemap(component_usage, title):
    """
    Create a treemap visualization of standard library usage.
    
    Args:
        component_usage (pd.DataFrame): The DataFrame containing the prepared data for the treemap.
        title (str): The title of the treemap.

    Returns:
        None: The function displays the treemap using Plotly.
    """
    hovertemplate = "%{label}<br>Count: %{customdata[0]}<br>Share of library: %{customdata[1]:.2f}%<br>Share of all: %{customdata[2]:.2f}%"

    fig = px.treemap(
        component_usage,
        path=['library_name', 'component'],
        values='count',
        color='library_percentage',
        custom_data=['count', 'library_percentage', 'total_percentage'],
        title=title,
        color_continuous_scale='RdBu',
        labels={'library_name': 'Library',
                'component': 'Component',
                'count': 'Count'}
    )
    
    fig.update_traces(hovertemplate=hovertemplate, textinfo='label+value+percent parent')

    fig.update_layout(
        width=1200,
        height=1200,
        uniformtext=dict(minsize=18, mode='show'),
        title={
            'text': title,
            'font': {'size': 30},
            'x': 0.5,
            'y': 0.98,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        margin=dict(l=10, r=10, t=100, b=10)
    )
    fig.show()



def plot_usage_in_files(df, library_name=None, top_n=20, number_format='p', component_types=None):
    """
    Plots number of files in which libraries or library components were used from given DataFrame.

    Parameters:
    df (DataFrame): DataFrame containing the library and component usage information.
    library_name (str, optional): Name of the library for which to plot component usage. If not provided, library usage will be plotted.
    top_n (int, optional): Number of top libraries or components to display in the plot. Default is 20.
    number_format (str, optional): Format for displaying numbers on the y-axis ('plain', 'k', 'M', '%' or 'p'). Default is 'p' for percentage.
    component_types (list, optional): List of component types to filter the data by. Default is None, which includes all component types.

    Returns:
    None
    """
    if library_name is None:
        groupby_column = 'library_name'
    else:
        groupby_column = 'component'
        df = df[df['library_name'] == library_name]
    
    if component_types is not None:
        df = df[df['component_type'].isin(component_types)]

    usage = df.groupby(groupby_column)['chunk_id'].nunique()
    usage.sort_values(ascending=False, inplace=True)

    if top_n is not None:
        usage = usage.head(top_n)

    if number_format in ('%', 'p'):
        total_files = df['chunk_id'].nunique()
        usage = (usage / total_files)
    elif number_format == 'k':
        usage = usage / 1000
    elif number_format == 'M':
        usage = usage / 1e6
        
    _, ax = plt.subplots(figsize=(20, 10))
    usage.plot(kind='bar', ax=ax)
    
    if number_format in ('%', 'p'):
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, pos: f'{x*100:.0f}%'))
    elif number_format == 'k':
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, pos: f'{x:.0f}k'))
    elif number_format == 'M':
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, pos: f'{x:.1f}M'))
    
    plt.ylabel('Number of Files' if number_format not in ('%', 'p') else 'Percentage of Files')
    plt.xlabel('Library' if library_name is None else 'Component')
    plt.title(f"{'Library' if library_name is None else 'Component'} Usage")
    plt.xticks(rotation=45, ha='right')
    plt.rcParams['font.size'] = 16
    plt.rcParams['axes.labelsize'] = 16
    plt.rcParams['axes.titlesize'] = 20
    plt.rcParams['xtick.labelsize'] = 16
    plt.rcParams['ytick.labelsize'] = 12
    plt.show()


def plot_usage_within_files(df, components_list, top_n=None, number_format='plain', merge_from_components=True):
    """
    Plots the usage of specified components within files.

    Parameters:
    df (DataFrame): DataFrame containing the library and component usage information.
    components_list (list): List of components to display in the plot.
    top_n (int, optional): Number of top libraries to display in the plot. Default is None, which displays all libraries.
    number_format (str, optional): Format for displaying numbers on the y-axis ('plain', 'k', or 'M'). Default is 'plain'.
    merge_from_components (bool, optional): If True, sums each component xyz with its corresponding from_import_xyz. Default is True.

    Returns:
    None
    """
    # Select only the specified components from the DataFrame
    df_selected = df[components_list].copy()
    
    # If merge_from_components is True, sum each component xyz with its corresponding from_import_xyz
    if merge_from_components:
        for component in components_list:
            from_component = f'from_import_{component}'
            if from_component in df.columns:
                df_selected[component] = df[component] + df[from_component]
                df_selected.drop(from_component, axis=1, errors='ignore', inplace=True)
    
    # If top_n is specified, select only the top_n libraries based on the sum of usage
    if top_n is not None:
        df_selected = df_selected.head(top_n)
    
    # Formatting according to the number_format
    if number_format == 'k':
        df_selected = df_selected / 1000
    elif number_format == 'M':
        df_selected = df_selected / 1e6
    
    _, ax = plt.subplots(figsize=(20, 10))
    df_selected.plot(kind='bar', ax=ax, width=1)
    
    if number_format == 'plain':
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, pos: f'{x:.0f}'))
    elif number_format == 'k':
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, pos: f'{x:.0f}k'))
    elif number_format == 'M':
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, pos: f'{x:.1f}M'))
    
    plt.ylabel('Number of Occurences')
    plt.xlabel('Library')
    plt.title('Components Usage by Library')
    plt.xticks(rotation=45, ha='right')
    plt.rcParams['font.size'] = 16
    plt.rcParams['axes.labelsize'] = 16
    plt.rcParams['axes.titlesize'] = 20
    plt.rcParams['xtick.labelsize'] = 16
    plt.rcParams['ytick.labelsize'] = 12
    plt.show()


def prepare_libraries_df_for_mean_complexity_plot(libraries):
    modified_libraries = libraries[['chunk_id', 'library_name']].drop_duplicates()
    modified_libraries['value'] = 1
    modified_libraries = modified_libraries.pivot_table(index='chunk_id', columns='library_name', values='value')
    return modified_libraries


def calculate_mean_complexity(df, metadata, min_count):
    """
    Calculate the mean complexity of code files for each function, given a minimum count of occurrences.

    Args:
        df (pd.DataFrame): DataFrame containing information about the functions or libraries used in code files.
        metadata (pd.DataFrame): DataFrame containing metadata, including complexity, for each code file.
        min_count (int): Minimum number of occurrences for a function to be included in the results.

    Returns:
        pd.DataFrame: DataFrame containing the mean complexity for each function, sorted by descending complexity.
    """
    if 'library_name' in df.columns:
        df = prepare_libraries_df_for_mean_complexity_plot(df)

    filtered = df.loc[:, df.notna().sum() >= min_count]
    combined_df = filtered.join(metadata['complexity'])
    
    mean_complexities = filtered.apply(lambda x: combined_df.loc[x.notna(), 'complexity'].mean(), axis=0)
    mean_complexities_df = mean_complexities.reset_index()
    mean_complexities_df.columns = ['function', 'mean_complexity']
    mean_complexities_df = mean_complexities_df.sort_values(by='mean_complexity', ascending=False)

    return mean_complexities_df


def plot_mean_complexity(df, metadata, min_count, title, kind='Function'):
    """
    Plot the mean complexity of code files by function, given a minimum count of occurrences.

    Args:
        df (pd.DataFrame): DataFrame containing information about the functions or libraries used in code files.
        metadata (pd.DataFrame): DataFrame containing metadata, including complexity, for each code file.
        min_count (int): Minimum number of occurrences for a function to be included in the results.
        title (str): Title for the plot.
    """
    mean_complexities_df = calculate_mean_complexity(df, metadata, min_count)

    plt.figure(figsize=(20, 10))
    sns.barplot(x='function', y='mean_complexity', data=mean_complexities_df)
    plt.xticks(rotation=90, fontsize=16)
    plt.yticks(fontsize=16)
    plt.xlabel(kind, fontsize=16)
    plt.ylabel('Mean Complexity', fontsize=16)
    plt.title(title)
    plt.show()
