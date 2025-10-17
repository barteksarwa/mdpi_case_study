
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from PIL import Image


def generate_fragments(image, bounding_box, n, size, inside, output_folder):
    # write your code here
    pass


def main():
    image = Image.open('cat.jpg')

    bounding_box = ((200, 730), (630, 120))
    n = 100
    size = 30
    inside = True
    output_folder = 'output'

    # code for viewing the bounding box
    fig, ax = plt.subplots()
    ax.imshow(image)
    width = bounding_box[1][0] - bounding_box[0][0]
    height = bounding_box[1][1] - bounding_box[0][1]
    rect = Rectangle(bounding_box[0], width, height, linewidth=1, edgecolor='r', facecolor='none')
    ax.add_patch(rect)
    plt.show()

    generate_fragments(image, bounding_box, n, size, inside, output_folder)


if __name__ == "__main__":
    main()


